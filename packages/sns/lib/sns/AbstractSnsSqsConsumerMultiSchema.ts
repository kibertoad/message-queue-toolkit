import type { SNSClient } from '@aws-sdk/client-sns'
import type { MultiSchemaConsumerOptions } from '@message-queue-toolkit/core'
import type { SQSCreationConfig } from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumerMultiSchema } from '@message-queue-toolkit/sqs/dist/lib/sqs/AbstractSqsConsumerMultiSchema'

import type { SNSCreationConfig } from './AbstractSnsService'
import type {
  ExistingSnsSqsConsumerOptions,
  NewSnsSqsConsumerOptions,
  SNSSQSConsumerDependencies,
  SNSSQSQueueLocatorType,
} from './AbstractSnsSqsConsumerMonoSchema'
import { initSns, initSnsSqs } from './SnsInitter'
import type { SNSSubscriptionOptions } from './SnsSubscriber'
import { deserializeSNSMessage } from './snsMessageDeserializer'
import {SQSMessage} from "@message-queue-toolkit/sqs";
import {ZodSchema} from "zod";
import {SNS_MESSAGE_BODY_TYPE} from "../types/MessageTypes";

export type ExistingSnsSqsConsumerOptionsMulti<
  MessagePayloadType extends object,
  ExecutionContext,
> = ExistingSnsSqsConsumerOptions<MessagePayloadType> &
  MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext>

export type NewSnsSqsConsumerOptionsMulti<
  MessagePayloadType extends object,
  ExecutionContext,
> = NewSnsSqsConsumerOptions<MessagePayloadType> &
  MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext>

export abstract class AbstractSnsSqsConsumerMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
> extends AbstractSqsConsumerMultiSchema<
  MessagePayloadSchemas,
  ExecutionContext,
  SNSSQSQueueLocatorType,
  SNSCreationConfig & SQSCreationConfig,
  | NewSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>
  | ExistingSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  // @ts-ignore
  public topicArn: string
  // @ts-ignore
  public subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options:
      | NewSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>
      | ExistingSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>,
  ) {
    super(dependencies, {
      ...options,
      deserializer: options.deserializer ?? deserializeSNSMessage,
    })

    this.subscriptionConfig = options.subscriptionConfig
    this.snsClient = dependencies.snsClient
  }

  async init(): Promise<void> {
    await super.init()

    const initSnsResult = await initSns(this.snsClient, this.locatorConfig, this.creationConfig)
    this.topicArn = initSnsResult.topicArn

    const initSnsSqsResult = await initSnsSqs(
      this.sqsClient,
      this.snsClient,
      this.locatorConfig,
      this.creationConfig,
      this.subscriptionConfig,
    )
    this.subscriptionArn = initSnsSqsResult.subscriptionArn
  }

  protected override resolveMessage(message: SQSMessage) {
    try {
      const snsMessage: SNS_MESSAGE_BODY_TYPE = JSON.parse(message.Body)
      const messagePayload = JSON.parse(snsMessage.Message)

      return {
        result: messagePayload
      }
    } catch (err) {
      return {
        error: this.errorResolver.processError(err),
      }
    }
  }

  protected override resolveSchema(messagePayload: unknown): ZodSchema<MessagePayloadSchemas> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.messageSchemaContainer.resolveSchema(messagePayload as Record<string, any>)
  }
}
