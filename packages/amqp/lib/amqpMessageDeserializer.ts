import type { Either } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ParseMessageResult,
} from '@message-queue-toolkit/core'
import { parseMessage } from '@message-queue-toolkit/core'
import type { Message } from 'amqplib'
import type { ZodType } from 'zod/v3'

import type { AmqpConsumerErrorResolver } from './errors/AmqpConsumerErrorResolver.ts'

export const deserializeAmqpMessage = <T extends object>(
  message: Message,
  type: ZodType<T>,
  errorProcessor: AmqpConsumerErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ParseMessageResult<T>> => {
  try {
    return parseMessage(JSON.parse(message.content.toString()), type, errorProcessor)
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
