import type { Either } from '@lokalise/node-core'
import type { BarrierResult, PreHandlingOutputs, Prehandler } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type {
  PubSubConsumerDependencies,
  PubSubConsumerOptions,
} from '../../lib/pubsub/AbstractPubSubConsumer.ts'
import { AbstractPubSubConsumer } from '../../lib/pubsub/AbstractPubSubConsumer.ts'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas.ts'

export type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

type PubSubPermissionConsumerOptions = Pick<
  PubSubConsumerOptions<SupportedMessages, ExecutionContext, PrehandlerOutput>,
  | 'creationConfig'
  | 'locatorConfig'
  | 'logMessages'
  | 'deletionConfig'
  | 'deadLetterQueue'
  | 'consumerOverrides'
  | 'maxRetryDuration'
  | 'payloadStoreConfig'
  | 'messageDeduplicationConfig'
  | 'enableConsumerDeduplication'
> & {
  addPreHandlerBarrier?: (
    message: SupportedMessages,
    _executionContext: ExecutionContext,
    preHandlerOutput: PrehandlerOutput,
  ) => Promise<BarrierResult<number>>
  removeHandlerOverride?: (
    _message: SupportedMessages,
    context: ExecutionContext,
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  addHandlerOverride?: (
    message: SupportedMessages,
    context: ExecutionContext,
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedMessages, ExecutionContext, PrehandlerOutput>[]
}

type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  messageId: string
}

export class PubSubPermissionConsumer extends AbstractPubSubConsumer<
  SupportedMessages,
  ExecutionContext,
  PrehandlerOutput
> {
  public addCounter = 0
  public removeCounter = 0
  public processedMessagesIds: Set<string> = new Set()
  public static readonly TOPIC_NAME = 'user_permissions'
  public static readonly SUBSCRIPTION_NAME = 'user_permissions_sub'

  constructor(
    dependencies: PubSubConsumerDependencies,
    options: PubSubPermissionConsumerOptions = {
      creationConfig: {
        topic: {
          name: PubSubPermissionConsumer.TOPIC_NAME,
        },
        subscription: {
          name: PubSubPermissionConsumer.SUBSCRIPTION_NAME,
        },
      },
    },
  ) {
    const defaultRemoveHandler = (
      _message: SupportedMessages,
      context: ExecutionContext,
      _preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      this.removeCounter += context.incrementAmount
      return Promise.resolve({
        result: 'success',
      })
    }

    const defaultAddHandler = (
      message: SupportedMessages,
      context: ExecutionContext,
      barrierOutput: PreHandlingOutputs<PrehandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      if (options.addPreHandlerBarrier && !barrierOutput) {
        return Promise.resolve({ error: 'retryLater' })
      }
      this.addCounter += context.incrementAmount
      this.processedMessagesIds.add(message.id)
      return Promise.resolve({ result: 'success' })
    }

    super(
      dependencies,
      {
        ...options,
        messageTypeField: 'messageType',
        handlerSpy: true,
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PrehandlerOutput
        >()
          .addConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
            options.removeHandlerOverride ?? defaultRemoveHandler,
            {
              preHandlers: options.removePreHandlers,
            },
          )
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            options.addHandlerOverride ?? defaultAddHandler,
            {
              preHandlerBarrier: options.addPreHandlerBarrier,
            },
          )
          .build(),
      },
      {
        incrementAmount: 1,
      },
    )
  }
}
