import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ParseMessageResult,
} from '@message-queue-toolkit/core'
import { parseMessage } from '@message-queue-toolkit/core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import type { ZodType } from 'zod'

import { SNS_MESSAGE_BODY_SCHEMA } from '../types/MessageTypes.ts'

export const deserializeSNSMessage = <T extends object>(
  message: SQSMessage,
  type: ZodType<T>,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ParseMessageResult<T>> => {
  try {
    const snsMessage = SNS_MESSAGE_BODY_SCHEMA.parse(JSON.parse(message.Body))

    return parseMessage(JSON.parse(snsMessage.Message), type, errorProcessor)
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
