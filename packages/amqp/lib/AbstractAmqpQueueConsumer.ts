import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  BarrierResult,
  DeadLetterQueueOptions,
  ParseMessageResult,
  Prehandler,
  PreHandlingOutputs,
  QueueConsumer,
  QueueConsumerOptions,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import {
  isRetryDateExceeded,
  isMessageError,
  parseMessage,
  HandlerContainer,
  MessageSchemaContainer,
} from '@message-queue-toolkit/core'
import type { Connection, Message } from 'amqplib'

import type {
  AMQPConsumerDependencies,
  AMQPLocator,
  AMQPCreationConfig,
} from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'
import { readAmqpMessage } from './amqpMessageReader'
import {AbstractAmqpConsumer} from "./AbstractAmqpConsumer";
import {undefined} from "zod";

export class AbstractAmqpQueueConsumer<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
> extends AbstractAmqpConsumer<MessagePayloadType, ExecutionContext, PrehandlerOutput> {

  protected async createMissingEntities(): Promise<void> {
    if (this.creationConfig) {
      await this.channel.assertQueue(
          this.creationConfig.queueName,
          this.creationConfig.queueOptions,
      )
    } else {
      await this.checkQueueExists()
    }
  }
}
