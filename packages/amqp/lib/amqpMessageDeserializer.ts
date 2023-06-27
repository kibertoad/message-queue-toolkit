import type { Either } from '@lokalise/node-core'
import type { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'
import type { Message } from 'amqplib'
import type { ZodType } from 'zod'

import type { AmqpConsumerErrorResolver } from './errors/AmqpConsumerErrorResolver'

export const deserializeAmqpMessage = <T extends object>(
  message: Message,
  type: ZodType<T>,
  errorProcessor: AmqpConsumerErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, T> => {
  try {
    return {
      result: type.parse(JSON.parse(message.content.toString())),
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
