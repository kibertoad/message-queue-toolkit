import { AbstractPeriodicJob, type JobExecutionContext } from '@lokalise/background-jobs-common'
import type { PeriodicJobDependencies } from '@lokalise/background-jobs-common/dist/periodic-jobs/periodicJobTypes'
import { generateUuid7 } from '@lokalise/id-utils'
import type {
  CommonEventDefinition,
  CommonEventDefinitionPublisherSchemaType,
  ConsumerMessageMetadataType,
  DomainEventEmitter,
} from '@message-queue-toolkit/core'

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

/**
 * Takes care of persisting and retrieving outbox entries.
 *
 * Implementation is required:
 * - in order to fulfill at least once delivery guarantee, persisting entries should be performed inside isolated transaction
 * - to return entries in the order they were created (UUID7 is used to create entries in OutboxEventEmitter)
 * - returned entries should not include the ones with 'SUCCESS' status
 */
export interface OutboxStorage<SupportedEvents extends CommonEventDefinition[]> {
  create(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>>

  update(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>>

  /**
   * Returns entries in the order they were created. It doesn't return entries with 'SUCCESS' status. It doesn't return entries that have been retried more than maxRetryCount times.
   *
   * For example if entry retryCount is 1 and maxRetryCount is 1, entry MUST be returned. If it fails again then retry count is 2, in that case entry MUST NOT be returned.
   */
  getEntries(maxRetryCount: number): Promise<OutboxEntry<SupportedEvents[number]>[]>
}

/**
 * Periodic job that processes outbox entries every second. If processing takes longer than 1 second, another subsequent job WILL NOT be started.
 *
 * Each entry is ACKed, then event is published, and then entry is marked as SUCCESS. If processing fails, entry is marked as FAILED and will be retried.
 *
 * Max retry count is defined by the user.
 */
export class OutboxPeriodicJob<
  SupportedEvents extends CommonEventDefinition[],
> extends AbstractPeriodicJob {
  constructor(
    private readonly outboxStorage: OutboxStorage<SupportedEvents>,
    private readonly eventEmitter: DomainEventEmitter<SupportedEvents>,
    private readonly maxRetryCount: number,
    dependencies: PeriodicJobDependencies,
  ) {
    super(
      {
        jobId: 'OutboxJob',
        schedule: {
          intervalInMs: 1000,
        },
        singleConsumerMode: {
          enabled: true,
        },
      },
      {
        redis: dependencies.redis,
        logger: dependencies.logger,
        transactionObservabilityManager: dependencies.transactionObservabilityManager,
        errorReporter: dependencies.errorReporter,
        scheduler: dependencies.scheduler,
      },
    )
  }

  protected async processInternal(context: JobExecutionContext): Promise<void> {
    const entries = await this.outboxStorage.getEntries(this.maxRetryCount)

    for (const entry of entries) {
      try {
        const updatedEntry = await this.outboxStorage.update({
          ...entry,
          updated: new Date(),
          status: 'ACKED',
        })

        await this.eventEmitter.emit(entry.event, entry.data, entry.precedingMessageMetadata)

        await this.outboxStorage.update({ ...updatedEntry, updated: new Date(), status: 'SUCCESS' })
      } catch (e) {
        context.logger.error({ error: e }, 'Failed to process outbox entry.')

        await this.outboxStorage.update({
          ...entry,
          updated: new Date(),
          status: 'FAILED',
          retryCount: entry.retryCount + 1,
        })
      }
    }
  }
}

export class OutboxEventEmitter<SupportedEvents extends CommonEventDefinition[]> {
  constructor(private storage: OutboxStorage<SupportedEvents>) {}

  /**
   * Persists outbox entry in persistence layer, later it will be picked up by outbox job.
   * @param supportedEvent
   * @param data
   * @param precedingMessageMetadata
   */
  public async emit<SupportedEvent extends SupportedEvents[number]>(
    supportedEvent: SupportedEvent,
    data: Omit<CommonEventDefinitionPublisherSchemaType<SupportedEvent>, 'type'>,
    precedingMessageMetadata?: Partial<ConsumerMessageMetadataType>,
  ) {
    await this.storage.create({
      id: generateUuid7(),
      event: supportedEvent,
      data,
      precedingMessageMetadata,
      status: 'CREATED',
      created: new Date(),
      retryCount: 0,
    })
  }
}
