import { z } from 'zod'

// Private interface to provide JSDoc feature
interface PublisherMessageDeduplicationMessageTypeInternal {
  /** How many seconds to keep the deduplication key in the store for a particular message type */
  deduplicationWindowSeconds: number
}

export const PUBLISHER_MESSAGE_DEDUPLICATION_MESSAGE_TYPE_SCHEMA = z.object({
  deduplicationWindowSeconds: z.number().int().gt(0),
})

export type PublisherMessageDeduplicationMessageType = z.infer<
  typeof PUBLISHER_MESSAGE_DEDUPLICATION_MESSAGE_TYPE_SCHEMA
> &
  PublisherMessageDeduplicationMessageTypeInternal

// Private interface to provide JSDoc feature
interface ConsumerMessageDeduplicationMessageTypeInternal {
  /** How many seconds to keep the deduplication key in the store for a particular message type after message is successfully processed */
  deduplicationWindowSeconds: number

  /** How many seconds it is expected to take to process a message of a particular type */
  maximumProcessingTimeSeconds: number
}

export const CONSUMER_MESSAGE_DEDUPLICATION_MESSAGE_TYPE_SCHEMA =
  PUBLISHER_MESSAGE_DEDUPLICATION_MESSAGE_TYPE_SCHEMA.extend({
    maximumProcessingTimeSeconds: z.number().int().gt(0),
  })

export type ConsumerMessageDeduplicationMessageType = z.infer<
  typeof CONSUMER_MESSAGE_DEDUPLICATION_MESSAGE_TYPE_SCHEMA
> &
  ConsumerMessageDeduplicationMessageTypeInternal
