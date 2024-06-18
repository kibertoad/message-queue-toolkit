import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ResolvedMessage,
} from '@message-queue-toolkit/core'
import type { Message } from 'amqplib'

export function readAmqpMessage(
  message: Message,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ResolvedMessage> {
  try {
    return {
      result: {
        body: JSON.parse(message.content.toString()),
      },
    }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
