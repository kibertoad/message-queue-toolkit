import type {
  CreateQueueCommandInput,
  QueueAttributeName,
  SendMessageCommandInput,
  SQSClient,
} from '@aws-sdk/client-sqs'
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
  SetQueueAttributesCommand,
} from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import { globalLogger } from '@lokalise/node-core'
import { isShallowSubset, waitAndRetry } from '@message-queue-toolkit/core'
import type { ExtraSQSCreationParams, SQSQueueLocatorType } from '../sqs/AbstractSqsService.ts'
import {
  generateQueuePolicyFromPolicyConfig,
  generateQueuePublishForTopicPolicy,
} from './sqsAttributeUtils.ts'
import { updateQueueAttributes, updateQueueTags } from './sqsInitter.ts'

const AWS_QUEUE_DOES_NOT_EXIST_ERROR_NAME = 'QueueDoesNotExist'

type QueueAttributesResult = {
  attributes?: Partial<Record<QueueAttributeName, string>>
}

export async function getQueueUrl(
  sqsClient: SQSClient,
  queueName: string,
): Promise<Either<'not_found', string>> {
  try {
    const result = await sqsClient.send(
      new GetQueueUrlCommand({
        QueueName: queueName,
      }),
    )

    if (result.QueueUrl) {
      return {
        result: result.QueueUrl,
      }
    }

    return {
      error: 'not_found',
    }
  } catch (err) {
    // @ts-expect-error
    if (err.Code === 'AWS.SimpleQueueService.NonExistentQueue') {
      return {
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function getQueueAttributes(
  sqsClient: SQSClient,
  queueUrl: string,
  attributeNames: QueueAttributeName[] = ['All'],
): Promise<Either<'not_found', QueueAttributesResult>> {
  const command = new GetQueueAttributesCommand({
    QueueUrl: queueUrl,
    AttributeNames: attributeNames,
  })

  try {
    const response = await sqsClient.send(command)
    return {
      result: {
        attributes: response.Attributes,
      },
    }
  } catch (err) {
    // @ts-expect-error
    if (err.Code === 'AWS.SimpleQueueService.NonExistentQueue') {
      return {
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function resolveQueueUrlFromLocatorConfig(
  sqsClient: SQSClient,
  locatorConfig: Partial<SQSQueueLocatorType>,
) {
  if (locatorConfig.queueUrl) {
    return locatorConfig.queueUrl
  }

  if (!locatorConfig.queueName) {
    throw new Error('Invalid locatorConfig setup - queueUrl or queueName must be provided')
  }

  const queueUrlResult = await getQueueUrl(sqsClient, locatorConfig.queueName)

  if (queueUrlResult.error === 'not_found') {
    throw new Error(`Queue with queueName ${locatorConfig.queueName} does not exist.`)
  }

  return queueUrlResult.result
}

async function updateExistingQueue(
  sqsClient: SQSClient,
  queueUrl: string,
  queueConfig: CreateQueueCommandInput,
  extraParams?: ExtraSQSCreationParams,
) {
  const existingAttributes = await getQueueAttributes(sqsClient, queueUrl)

  if (!existingAttributes.result?.attributes) {
    throw new Error('Attributes are not set')
  }

  const queueArn = existingAttributes.result?.attributes.QueueArn
  if (!queueArn) {
    throw new Error('Queue ARN was not set')
  }

  // we will try to update existing queue if exists
  if (extraParams?.updateAttributesIfExists) {
    const updatedAttributes: Partial<Record<QueueAttributeName, string>> = {
      ...queueConfig.Attributes,
    }

    if (extraParams.policyConfig) {
      updatedAttributes.Policy = generateQueuePolicyFromPolicyConfig(
        queueArn,
        extraParams.policyConfig,
      )
    } else if (extraParams?.topicArnsWithPublishPermissionsPrefix) {
      updatedAttributes.Policy = generateQueuePublishForTopicPolicy(
        queueArn,
        extraParams.topicArnsWithPublishPermissionsPrefix,
      )
    }

    // Only perform update if there are new or changed values in the queue config
    if (!isShallowSubset(updatedAttributes, existingAttributes.result.attributes)) {
      await updateQueueAttributes(sqsClient, queueUrl, updatedAttributes)
    }
  }

  if (extraParams?.forceTagUpdate) {
    await updateQueueTags(sqsClient, queueUrl, queueConfig.tags)
  }

  return {
    queueUrl,
    queueArn,
    queueName: queueConfig.QueueName,
  }
}

export async function assertQueue(
  sqsClient: SQSClient,
  queueConfig: CreateQueueCommandInput,
  extraParams?: ExtraSQSCreationParams,
  isFifoQueue?: boolean,
) {
  // biome-ignore lint/style/noNonNullAssertion: It's ok
  const queueName = queueConfig.QueueName!

  // Validate FIFO queue configuration before creating/updating
  validateFifoQueueConfiguration(queueName, queueConfig.Attributes, isFifoQueue)

  const queueUrlResult = await getQueueUrl(sqsClient, queueName)
  const queueExists = !!queueUrlResult.result

  if (queueExists) {
    return updateExistingQueue(sqsClient, queueUrlResult.result, queueConfig, extraParams)
  }

  // create new queue
  const command = new CreateQueueCommand(queueConfig)
  await sqsClient.send(command)

  // biome-ignore lint/style/noNonNullAssertion: It's ok
  const newQueueUrlResult = await getQueueUrl(sqsClient, queueConfig.QueueName!)
  const newQueueExists = !!newQueueUrlResult.result

  if (!newQueueExists) {
    throw new Error(`Queue ${queueConfig.QueueName ?? ''} was not created`)
  }

  const getQueueAttributesCommand = new GetQueueAttributesCommand({
    QueueUrl: newQueueUrlResult.result,
    AttributeNames: ['QueueArn'],
  })
  const queueAttributesResponse = await sqsClient.send(getQueueAttributesCommand)
  const queueArn = queueAttributesResponse.Attributes?.QueueArn

  if (!queueArn) {
    throw new Error('Queue ARN was not set')
  }

  if (extraParams?.policyConfig) {
    await sqsClient.send(
      new SetQueueAttributesCommand({
        QueueUrl: newQueueUrlResult.result,
        Attributes: {
          Policy: generateQueuePolicyFromPolicyConfig(queueArn, extraParams.policyConfig),
        },
      }),
    )
  } else if (extraParams?.topicArnsWithPublishPermissionsPrefix) {
    await sqsClient.send(
      new SetQueueAttributesCommand({
        QueueUrl: newQueueUrlResult.result,
        Attributes: {
          Policy: generateQueuePublishForTopicPolicy(
            queueArn,
            extraParams.topicArnsWithPublishPermissionsPrefix,
          ),
        },
      }),
    )
  }

  return {
    queueArn,
    queueUrl: newQueueUrlResult.result,
    queueName: queueConfig.QueueName,
  }
}

export async function deleteQueue(
  client: SQSClient,
  queueName: string,
  waitForConfirmation = true,
) {
  try {
    const queueUrlCommand = new GetQueueUrlCommand({
      QueueName: queueName,
    })
    const response = await client.send(queueUrlCommand)

    const command = new DeleteQueueCommand({
      QueueUrl: response.QueueUrl,
    })

    await client.send(command)

    if (waitForConfirmation) {
      await waitAndRetry(async () => {
        const queueList = await client.send(
          new ListQueuesCommand({
            QueueNamePrefix: queueName,
          }),
        )
        return !queueList.QueueUrls || queueList.QueueUrls.length === 0
      })
    }
  } catch (err) {
    // This is fine
    // @ts-expect-error
    if (err.name === AWS_QUEUE_DOES_NOT_EXIST_ERROR_NAME) {
      return
    }

    // @ts-expect-error
    globalLogger.error(`Failed to delete: ${err.message}`)
  }
}

/**
 * Calculates the size of an outgoing SQS message.
 *
 * SQS imposes a 256 KB limit on the total size of a message, which includes both the message body and any metadata (attributes).
 * This function currently computes the size based solely on the message body, as no attributes are included at this time.
 * For future updates, if message attributes are added, their sizes should also be considered.
 *
 * Reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes
 */
export function calculateOutgoingMessageSize(message: unknown) {
  const messageBody = JSON.stringify(message)
  return calculateSqsMessageBodySize(messageBody)
}

export function calculateSqsMessageBodySize(messageBody: SendMessageCommandInput['MessageBody']) {
  return messageBody ? Buffer.byteLength(messageBody, 'utf8') : 0
}

/**
 * Checks if a queue name indicates a FIFO queue (ends with .fifo)
 */
export function isFifoQueueName(queueName: string): boolean {
  return queueName.endsWith('.fifo')
}

/**
 * Validates that queue name matches the FIFO configuration flag
 */
export function validateFifoQueueName(queueName: string, isFifoQueue: boolean): void {
  const hasFifoName = isFifoQueueName(queueName)

  if (isFifoQueue && !hasFifoName) {
    throw new Error(`FIFO queue names must end with .fifo suffix. Queue name: ${queueName}`)
  }

  if (!isFifoQueue && hasFifoName) {
    throw new Error(
      `Queue name ends with .fifo but fifoQueue option is not set to true. Queue name: ${queueName}`,
    )
  }
}

/**
 * Validates FIFO queue configuration for creation
 * - FIFO queues must have names ending with .fifo
 * - If FifoQueue attribute is 'true', name must end with .fifo
 */
export function validateFifoQueueConfiguration(
  queueName: string,
  attributes?: Partial<Record<QueueAttributeName, string>>,
  isFifoQueue?: boolean,
): void {
  const hasFifoAttribute = attributes?.FifoQueue === 'true'
  const hasFifoName = isFifoQueueName(queueName)

  // If explicit FIFO flag is provided, validate against it
  if (isFifoQueue !== undefined) {
    validateFifoQueueName(queueName, isFifoQueue)

    // Also validate that attributes match if provided
    if (attributes && hasFifoAttribute !== isFifoQueue) {
      throw new Error(
        `FifoQueue attribute (${hasFifoAttribute}) does not match fifoQueue option (${isFifoQueue})`,
      )
    }
  }

  // Validate consistency between name and attributes
  if (hasFifoAttribute && !hasFifoName) {
    throw new Error(`FIFO queue names must end with .fifo suffix. Queue name: ${queueName}`)
  }

  if (hasFifoName && attributes && !hasFifoAttribute) {
    throw new Error(
      `Queue name ends with .fifo but FifoQueue attribute is not set to 'true'. Queue name: ${queueName}`,
    )
  }
}

/**
 * Detects if a queue is FIFO based on its attributes
 */
export async function detectFifoQueue(
  sqsClient: SQSClient,
  queueUrl: string,
  queueName?: string,
): Promise<boolean> {
  // First check the queue name if provided
  if (queueName && isFifoQueueName(queueName)) {
    return true
  }

  // Fallback to checking queue attributes
  const attributesResult = await getQueueAttributes(sqsClient, queueUrl, ['FifoQueue'])

  if (attributesResult.error) {
    return false
  }

  return attributesResult.result?.attributes?.FifoQueue === 'true'
}
