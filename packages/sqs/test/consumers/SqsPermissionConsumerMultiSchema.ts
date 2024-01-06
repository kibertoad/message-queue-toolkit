import type { Either } from '@lokalise/node-core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { BarrierResult, Prehandler } from '@message-queue-toolkit/core'

import type { SQSCreationConfig } from '../../lib/sqs/AbstractSqsConsumer'
import type {
  ExistingSQSConsumerOptionsMultiSchema,
  NewSQSConsumerOptionsMultiSchema,
} from '../../lib/sqs/AbstractSqsConsumerMultiSchema'
import { AbstractSqsConsumerMultiSchema } from '../../lib/sqs/AbstractSqsConsumerMultiSchema'
import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsService'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas'

type SqsPermissionConsumerMultiSchemaOptions = (
  | Pick<
      NewSQSConsumerOptionsMultiSchema<
        SupportedMessages,
        ExecutionContext,
        PrehandlerOutput,
        SQSCreationConfig
      >,
      'creationConfig' | 'logMessages'
    >
  | Pick<
      ExistingSQSConsumerOptionsMultiSchema<SupportedMessages, ExecutionContext, PrehandlerOutput>,
      'locatorConfig' | 'logMessages'
    >
) & {
  addPreHandlerBarrier?: (message: SupportedMessages) => Promise<BarrierResult<number>>
  removeHandlerOverride?: (
    _message: SupportedMessages,
    context: ExecutionContext,
    prehandlerOutput: PrehandlerOutput,
    barrierOutput: number,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedMessages, ExecutionContext, PrehandlerOutput>[]
}

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  messageId: string
}

export class SqsPermissionConsumerMultiSchema extends AbstractSqsConsumerMultiSchema<
  SupportedMessages,
  ExecutionContext,
  PrehandlerOutput
> {
  public addCounter = 0
  public removeCounter = 0
  public static QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SQSConsumerDependencies,
    options: SqsPermissionConsumerMultiSchemaOptions = {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionConsumerMultiSchema.QUEUE_NAME,
        },
      },
    },
  ) {
    const defaultRemoveHandler = async (
      message: SupportedMessages,
      context: ExecutionContext,
      _prehandlerOutput: PrehandlerOutput,
      _barrierOutput: number,
    ): Promise<Either<'retryLater', 'success'>> => {
      this.removeCounter += context.incrementAmount
      return {
        result: 'success',
      }
    }

    super(
      dependencies,
      {
        messageTypeField: 'messageType',
        handlerSpy: true,
        deletionConfig: {
          deleteIfExists: true,
        },
        consumerOverrides: {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        // FixMe this casting shouldn't be necessary
        ...(options as Pick<
          NewSQSConsumerOptionsMultiSchema<
            SupportedMessages,
            ExecutionContext,
            PrehandlerOutput,
            SQSCreationConfig
          >,
          'creationConfig' | 'logMessages'
        >),
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PrehandlerOutput
        >()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            async (_message, context, barrierOutput) => {
              if (options.addPreHandlerBarrier && !barrierOutput) {
                return { error: 'retryLater' }
              }
              this.addCounter += context.incrementAmount
              return {
                result: 'success',
              }
            },
            {
              preHandlerBarrier: options.addPreHandlerBarrier,
              prehandlers: [
                (message, _context, prehandlerOutput, next) => {
                  prehandlerOutput.messageId = message.id
                  next()
                },
              ],
            },
          )
          .addConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
            options.removeHandlerOverride ?? defaultRemoveHandler,
            {
              prehandlers: options.removePreHandlers,
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
