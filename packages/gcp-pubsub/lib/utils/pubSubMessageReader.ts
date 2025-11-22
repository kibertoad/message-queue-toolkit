import type { Either } from '@lokalise/node-core'
import { MessageInvalidFormatError } from '@message-queue-toolkit/core'
import type { PubSubMessage } from '../types/MessageTypes.ts'

export function readPubSubMessage(
  message: PubSubMessage,
): Either<MessageInvalidFormatError, unknown> {
  try {
    const messageData = message.data.toString()
    const parsedData = JSON.parse(messageData)
    return { result: parsedData }
  } catch (error) {
    return {
      error: new MessageInvalidFormatError({
        message: 'Invalid message format',
        details: {
          messageId: message.id,
          processingError: (error as Error).message,
        },
      }),
    }
  }
}
