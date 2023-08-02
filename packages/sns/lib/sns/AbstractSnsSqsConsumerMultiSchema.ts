import type { SNSClient } from '@aws-sdk/client-sns'
import type { MultiSchemaConsumerOptions } from '@message-queue-toolkit/core'
import { HandlerContainer, MessageSchemaContainer } from '@message-queue-toolkit/core'
import type { SQSCreationConfig, SQSMessage } from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer } from '@message-queue-toolkit/sqs'
import type { ZodSchema } from 'zod'

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
import {Either} from "@lokalise/node-core";
import {undefined} from "zod";
import {AbstractSqsConsumerMultiSchema} from "@message-queue-toolkit/sqs/dist/lib/sqs/AbstractSqsConsumerMultiSchema";

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
}
