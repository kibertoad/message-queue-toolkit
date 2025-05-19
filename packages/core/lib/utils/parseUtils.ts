import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod/v3'

import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors.ts'

export type ParseMessageResult<T> = {
  originalMessage: T
  parsedMessage: T
}

/**
 * Parses a message payload using the provided schema and error processor to handle validation.
 * The function attempts to parse the message payload based on the provided schema.
 * If successful, it returns a success object containing the parsed message along with the original message.
 * If an error occurs during parsing, it returns an error object processed by the error processor.
 *
 * In success case, it returns original and parsed messages because zod is cutting off additional fields that are not
 * defined in the schema, and we might add extra fields on publish to be used on the consumer side.
 * eg: timestamp to avoid infinite retries on the same message.
 *
 * @param messagePayload The message payload to be parsed.
 * @param schema The Zod schema used for parsing the message payload.
 * @param errorProcessor An error resolver function used to process any validation errors encountered during parsing.
 *
 * @returns Either an object indicating a successful parsing result or an error encountered during parsing.
 */
export function parseMessage<T extends object>(
  messagePayload: unknown,
  schema: ZodSchema<T>,
  errorProcessor: ErrorResolver,
): Either<MessageInvalidFormatError | MessageValidationError, ParseMessageResult<T>> {
  try {
    const parsedMessage = schema.parse(messagePayload)

    return { result: { parsedMessage, originalMessage: messagePayload as T } }
  } catch (exception) {
    return {
      error: errorProcessor.processError(exception),
    }
  }
}
