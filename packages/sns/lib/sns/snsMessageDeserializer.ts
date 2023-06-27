import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { ZodType } from 'zod'
import { SnsMessageInvalidFormat, SnsValidationError } from '../errors/snsErrors'
import { SNSMessageBodySchema } from '../types/MessageTypes'

export type SNSMessage = {
  MessageId: string
  ReceiptHandle: string
  MD5OfBody: string
  Body: string
}

export const deserializeSNSMessage = <T extends object>(
  message: SNSMessage,
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
