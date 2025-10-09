import type { Either, ErrorResolver } from '@lokalise/node-core'
import { isMessageError, type MessageInvalidFormatError } from '@message-queue-toolkit/core'
import type { PubSubMessage } from '../types/MessageTypes.ts'
import { readPubSubMessage } from './pubSubMessageReader.ts'

export function deserializePubSubMessage(
  message: PubSubMessage,
  errorResolver: ErrorResolver,
): Either<MessageInvalidFormatError, unknown> {
  const readResult = readPubSubMessage(message)

  if ('error' in readResult) {
    const resolvedError = errorResolver.processError(readResult.error)
    if (isMessageError(resolvedError)) {
      return { error: resolvedError }
    }
    return readResult
  }

  return readResult
}
