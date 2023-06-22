import type { Either } from '@lokalise/node-core'
import type { Message } from 'amqplib'
import type { ZodType } from 'zod'

import type { AmqpConsumerErrorResolver } from './errors/AmqpConsumerErrorResolver'
import type { AmqpMessageInvalidFormat, AmqpValidationError } from './errors/amqpErrors'

export const deserializeMessage = <T extends object>(
  message: Message,
  type: ZodType<T>,
  errorProcessor: AmqpConsumerErrorResolver,
): Either<AmqpMessageInvalidFormat | AmqpValidationError, T> => {
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
