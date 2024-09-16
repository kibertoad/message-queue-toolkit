import type {
  CommonEventDefinition,
  CommonEventDefinitionPublisherSchemaType,
  ConsumerMessageMetadataType,
} from '@message-queue-toolkit/schemas'

/**
 * Status of the outbox entry.
 * - CREATED - entry was created and is waiting to be processed to publish actual event
 * - ACKED - entry was picked up by outbox job and is being processed
 * - SUCCESS - entry was successfully processed, event was published
 * - FAILED - entry processing failed, it will be retried
 */
export type OutboxEntryStatus = 'CREATED' | 'ACKED' | 'SUCCESS' | 'FAILED'

export type OutboxEntry<SupportedEvent extends CommonEventDefinition> = {
  id: string
  event: SupportedEvent
  data: Omit<CommonEventDefinitionPublisherSchemaType<SupportedEvent>, 'type'>
  precedingMessageMetadata?: Partial<ConsumerMessageMetadataType>
  status: OutboxEntryStatus
  created: Date
  updated?: Date
  retryCount: number
}
