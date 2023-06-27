import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import type { ZodType } from 'zod'

import type { SnsMessageInvalidFormat, SnsValidationError } from '../errors/snsErrors'
import { SNSMessageBodySchema } from '../types/MessageTypes'

export const deserializeSNSMessage = <T extends object>(
  message: SQSMessage,
  type: ZodType<T>,
  errorProcessor: ErrorResolver,
): Either<SnsMessageInvalidFormat | SnsValidationError, T> => {
  try {
    const sqsMessage = SNSMessageBodySchema.parse(JSON.parse(message.Body))
    return {
      result: type.parse(JSON.parse(sqsMessage.Message)),
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
