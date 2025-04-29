import type { PeriodicJobDependencies } from '@lokalise/background-jobs-common'
import { AbstractPeriodicJob, type JobExecutionContext } from '@lokalise/background-jobs-common'
import type {
  CommonEventDefinition,
  CommonEventDefinitionPublisherSchemaType,
  ConsumerMessageMetadataType,
  DomainEventEmitter,
} from '@message-queue-toolkit/core'
import { PromisePool } from '@supercharge/promise-pool'
import { uuidv7 } from 'uuidv7'
import type { OutboxAccumulator } from './accumulators.ts'
import type { OutboxEntry } from './objects.ts'
import type { OutboxStorage } from './storage.ts'

export type OutboxDependencies<SupportedEvents extends CommonEventDefinition[]> = {
  outboxStorage: OutboxStorage<SupportedEvents>
  outboxAccumulator: OutboxAccumulator<SupportedEvents>
  eventEmitter: DomainEventEmitter<SupportedEvents>
}

export type OutboxProcessorConfiguration = {
  maxRetryCount: number
  emitBatchSize: number
}

export type OutboxConfiguration = {
  jobIntervalInMs: number
} & OutboxProcessorConfiguration

/**
 * Main logic for handling outbox entries.
 *
 * If entry is rejected, it is NOT going to be handled during the same execution. Next execution will pick it up.
 */
export class OutboxProcessor<SupportedEvents extends CommonEventDefinition[]> {
  private readonly outboxDependencies: OutboxDependencies<SupportedEvents>
  private readonly outboxProcessorConfiguration: OutboxProcessorConfiguration

  constructor(
    outboxDependencies: OutboxDependencies<SupportedEvents>,
    outboxProcessorConfiguration: OutboxProcessorConfiguration,
  ) {
    this.outboxDependencies = outboxDependencies
    this.outboxProcessorConfiguration = outboxProcessorConfiguration
  }

  public async processOutboxEntries(context: JobExecutionContext) {
    const { outboxStorage, eventEmitter, outboxAccumulator } = this.outboxDependencies

    const entries = await outboxStorage.getEntries(this.outboxProcessorConfiguration.maxRetryCount)

    const filteredEntries =
      entries.length === 0 ? entries : await this.getFilteredEntries(entries, outboxAccumulator)

    await PromisePool.for(filteredEntries)
      .withConcurrency(this.outboxProcessorConfiguration.emitBatchSize)
      .process(async (entry) => {
        try {
          await eventEmitter.emit(entry.event, entry.data, entry.precedingMessageMetadata)
          await outboxAccumulator.add(entry)
        } catch (e) {
          context.logger.error({ error: e }, 'Failed to process outbox entry.')

          await outboxAccumulator.addFailure(entry)
        }
      })

    await outboxStorage.flush(outboxAccumulator)
    await outboxAccumulator.clear()
  }

  private async getFilteredEntries(
    entries: OutboxEntry<SupportedEvents[number]>[],
    outboxAccumulator: OutboxAccumulator<SupportedEvents>,
  ) {
    const currentEntriesInAccumulator = new Set(
      (await outboxAccumulator.getEntries()).map((entry) => entry.id),
    )
    return entries.filter((entry) => !currentEntriesInAccumulator.has(entry.id))
  }
}

/**
 * Periodic job that processes outbox entries every "intervalInMs". If processing takes longer than defined interval, another subsequent job WILL NOT be started.
 *
 * When event is published, and then entry is accumulated into SUCCESS group. If processing fails, entry is accumulated as FAILED and will be retried.
 *
 * Max retry count is defined by the user.
 */
/* c8 ignore start */
export class OutboxPeriodicJob<
  SupportedEvents extends CommonEventDefinition[],
> extends AbstractPeriodicJob {
  private readonly outboxProcessor: OutboxProcessor<SupportedEvents>

  constructor(
    outboxDependencies: OutboxDependencies<SupportedEvents>,
    outboxConfiguration: OutboxConfiguration,
    dependencies: PeriodicJobDependencies,
  ) {
    super(
      {
        jobId: 'OutboxJob',
        schedule: {
          intervalInMs: outboxConfiguration.jobIntervalInMs,
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

    this.outboxProcessor = new OutboxProcessor<SupportedEvents>(
      outboxDependencies,
      outboxConfiguration,
    )
  }

  protected async processInternal(context: JobExecutionContext): Promise<void> {
    await this.outboxProcessor.processOutboxEntries(context)
  }
}
/* c8 ignore stop */

export class OutboxEventEmitter<SupportedEvents extends CommonEventDefinition[]> {
  private storage: OutboxStorage<SupportedEvents>

  constructor(storage: OutboxStorage<SupportedEvents>) {
    this.storage = storage
  }

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
    await this.storage.createEntry({
      id: uuidv7(),
      event: supportedEvent,
      data,
      precedingMessageMetadata,
      status: 'CREATED',
      created: new Date(),
      retryCount: 0,
    })
  }
}
