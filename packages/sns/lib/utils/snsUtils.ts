import {
  CreateTopicCommand,
  type CreateTopicCommandInput,
  DeleteTopicCommand,
  GetSubscriptionAttributesCommand,
  GetTopicAttributesCommand,
  ListSubscriptionsByTopicCommand,
  ListTagsForResourceCommand,
  SetTopicAttributesCommand,
  type SNSClient,
  TagResourceCommand,
  UnsubscribeCommand,
} from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'
import { type Either, InternalError, isError } from '@lokalise/node-core'
import { calculateOutgoingMessageSize as sqsCalculateOutgoingMessageSize } from '@message-queue-toolkit/sqs'
import type { ExtraSNSCreationParams } from '../sns/AbstractSnsService.ts'
import { generateTopicSubscriptionPolicy } from './snsAttributeUtils.ts'
import { buildTopicArn } from './stsUtils.ts'

type AttributesResult = {
  attributes?: Record<string, string>
}

export async function getTopicAttributes(
  snsClient: SNSClient,
  topicArn: string,
): Promise<Either<'not_found', AttributesResult>> {
  const command = new GetTopicAttributesCommand({
    TopicArn: topicArn,
  })

  try {
    const response = await snsClient.send(command)
    return {
      result: {
        attributes: response.Attributes,
      },
    }
  } catch (err) {
    // SNS returns NotFoundException for non-existent topics
    // @ts-expect-error
    if (err.name === 'NotFoundException' || err.name === 'NotFound') {
      return {
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function getSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
): Promise<Either<'not_found', AttributesResult>> {
  const command = new GetSubscriptionAttributesCommand({
    SubscriptionArn: subscriptionArn,
  })

  try {
    const response = await snsClient.send(command)
    return {
      result: {
        attributes: response.Attributes,
      },
    }
  } catch (err) {
    // SNS returns NotFoundException for non-existent subscriptions
    // @ts-expect-error
    if (err.name === 'NotFoundException' || err.name === 'NotFound') {
      return {
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function assertTopic(
  snsClient: SNSClient,
  stsClient: STSClient,
  topicOptions: CreateTopicCommandInput,
  extraParams?: ExtraSNSCreationParams,
) {
  let topicArn: string
  try {
    const command = new CreateTopicCommand(topicOptions)
    const response = await snsClient.send(command)
    if (!response.TopicArn) throw new Error('No topic arn in response')
    topicArn = response.TopicArn
  } catch (error) {
    if (!isError(error)) throw error
    // To build ARN we need topic name and error should be "topic already exist with different tags"
    if (!topicOptions.Name || !isTopicAlreadyExistWithDifferentTagsError(error)) {
      throw new InternalError({
        message: `${topicOptions.Name} - ${error.message}`,
        cause: error,
        details: { topicName: topicOptions.Name },
        errorCode: 'SNS_CREATE_TOPIC_COMMAND_UNEXPECTED_ERROR',
      })
    }

    topicArn = await buildTopicArn(stsClient, topicOptions.Name)
    if (!extraParams?.forceTagUpdate) {
      const currentTags = await snsClient.send(
        new ListTagsForResourceCommand({ ResourceArn: topicArn }),
      )
      throw new InternalError({
        message: `${topicOptions.Name} - ${error.message}`,
        details: {
          topicName: topicOptions.Name,
          currentTags: JSON.stringify(currentTags),
          newTags: JSON.stringify(topicOptions.Tags),
        },
        errorCode: 'SNS_TOPIC_ALREADY_EXISTS_WITH_DIFFERENT_TAGS',
        cause: error,
      })
    }
  }

  if (extraParams?.queueUrlsWithSubscribePermissionsPrefix || extraParams?.allowedSourceOwner) {
    const setTopicAttributesCommand = new SetTopicAttributesCommand({
      TopicArn: topicArn,
      AttributeName: 'Policy',
      AttributeValue: generateTopicSubscriptionPolicy({
        topicArn,
        allowedSqsQueueUrlPrefix: extraParams.queueUrlsWithSubscribePermissionsPrefix,
        allowedSourceOwner: extraParams.allowedSourceOwner,
      }),
    })
    await snsClient.send(setTopicAttributesCommand)
  }
  if (extraParams?.forceTagUpdate && topicOptions.Tags) {
    const tagTopicCommand = new TagResourceCommand({
      ResourceArn: topicArn,
      Tags: topicOptions.Tags,
    })
    await snsClient.send(tagTopicCommand)
  }

  return topicArn
}

export async function deleteTopic(snsClient: SNSClient, stsClient: STSClient, topicName: string) {
  try {
    const topicArn = await assertTopic(snsClient, stsClient, {
      Name: topicName,
    })

    await snsClient.send(
      new DeleteTopicCommand({
        TopicArn: topicArn,
      }),
    )
  } catch (_) {
    // we don't care it operation has failed
  }
}

export async function deleteSubscription(client: SNSClient, subscriptionArn: string) {
  const command = new UnsubscribeCommand({
    SubscriptionArn: subscriptionArn,
  })
  try {
    await client.send(command)
  } catch (_) {
    // we don't care it operation has failed
  }
}

export async function findSubscriptionByTopicAndQueue(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string,
) {
  const listSubscriptionsCommand = new ListSubscriptionsByTopicCommand({
    TopicArn: topicArn,
  })

  const listSubscriptionResult = await snsClient.send(listSubscriptionsCommand)
  return listSubscriptionResult.Subscriptions?.find((entry) => {
    return entry.Endpoint === queueArn
  })
}

/**
 * Calculates the size of an outgoing SNS message.
 *
 * SNS imposes a 256 KB limit on the total size of a message, which includes both the message body and any metadata (attributes).
 * This function computes the size based solely on the message body. The reserved buffer for message attributes and overhead
 * is accounted for in SNS_MESSAGE_ATTRIBUTE_BUFFER, which is subtracted from SNS_MESSAGE_HARD_LIMIT to derive SNS_MESSAGE_MAX_SIZE.
 *
 * Reference: https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
 *
 * A wrapper around the equivalent function in the SQS package.
 */
export const calculateOutgoingMessageSize = (message: unknown) =>
  sqsCalculateOutgoingMessageSize(message)

/**
 * Checks if a topic name indicates a FIFO topic (ends with .fifo)
 */
export function isFifoTopicName(topicName: string): boolean {
  return topicName.endsWith('.fifo')
}

/**
 * Validates that topic name matches the FIFO configuration flag
 */
export function validateFifoTopicName(topicName: string, isFifoTopic: boolean): void {
  const hasFifoName = isFifoTopicName(topicName)

  if (isFifoTopic && !hasFifoName) {
    throw new Error(`FIFO topic names must end with .fifo suffix. Topic name: ${topicName}`)
  }

  if (!isFifoTopic && hasFifoName) {
    throw new Error(
      `Topic name ends with .fifo but fifoTopic option is not set to true. Topic name: ${topicName}`,
    )
  }
}

const isTopicAlreadyExistWithDifferentTagsError = (error: unknown) =>
  !!error &&
  isError(error) &&
  'Error' in error &&
  !!error.Error &&
  typeof error.Error === 'object' &&
  'Code' in error.Error &&
  'Message' in error.Error &&
  typeof error.Error.Message === 'string' &&
  error.Error.Code === 'InvalidParameter' &&
  error.Error.Message.includes('already exists with different tags')
