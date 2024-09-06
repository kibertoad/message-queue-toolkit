import { AbstractPeriodicJob, type JobExecutionContext } from '@lokalise/background-jobs-common'
import type { PeriodicJobDependencies } from '@lokalise/background-jobs-common'
import type {
  CommonEventDefinition,
  CommonEventDefinitionPublisherSchemaType,
  ConsumerMessageMetadataType,
  DomainEventEmitter,
} from '@message-queue-toolkit/core'
import { PromisePool } from '@supercharge/promise-pool'
import { uuidv7 } from 'uuidv7'
import type { OutboxAccumulator } from './accumulators'
import type { OutboxStorage } from './storage'

/**
 * Main logic for handling outbox entries.
 *
 * If entry is rejected, it is NOT going to be handled during the same execution. Next execution will pick it up.
 */
export class OutboxProcessor<SupportedEvents extends CommonEventDefinition[]> {
  constructor(
    private readonly outboxStorage: OutboxStorage<SupportedEvents>,
    private readonly outboxAccumulator: OutboxAccumulator<SupportedEvents>,
    private readonly eventEmitter: DomainEventEmitter<SupportedEvents>,
    private readonly maxRetryCount: number,
    private readonly emitBatchSize: number,
  ) {}

  public async processOutboxEntries(context: JobExecutionContext) {
    const entries = await this.outboxStorage.getEntries(this.maxRetryCount)

    await PromisePool.for(entries)
      .withConcurrency(this.emitBatchSize)
      .process(async (entry) => {
        try {
          await this.eventEmitter.emit(entry.event, entry.data, entry.precedingMessageMetadata)
          await this.outboxAccumulator.add(entry)
        } catch (e) {
          context.logger.error({ error: e }, 'Failed to process outbox entry.')

          await this.outboxAccumulator.addFailure(entry)
        }
      })

    await this.outboxStorage.flush(this.outboxAccumulator)
    await this.outboxAccumulator.clear()
  }
}

/**
 * Periodic job that processes outbox entries every "intervalInMs". If processing takes longer than defined interval, another subsequent job WILL NOT be started.
 *
 * Each entry is ACKed, then event is published, and then entry is marked as SUCCESS. If processing fails, entry is marked as FAILED and will be retried.
 *
 * Max retry count is defined by the user.
 */
export class OutboxPeriodicJob<
  SupportedEvents extends CommonEventDefinition[],
> extends AbstractPeriodicJob {
  private readonly outboxProcessor: OutboxProcessor<SupportedEvents>

  constructor(
    outboxStorage: OutboxStorage<SupportedEvents>,
    outboxAccumulator: OutboxAccumulator<SupportedEvents>,
    eventEmitter: DomainEventEmitter<SupportedEvents>,
    dependencies: PeriodicJobDependencies,
    maxRetryCount: number,
    emitBatchSize: number,
    intervalInMs: number,
  ) {
    super(
      {
        jobId: 'OutboxJob',
        schedule: {
          intervalInMs: intervalInMs,
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
      outboxStorage,
      outboxAccumulator,
      eventEmitter,
      maxRetryCount,
      emitBatchSize,
    )
  }

  protected async processInternal(context: JobExecutionContext): Promise<void> {
    await this.outboxProcessor.processOutboxEntries(context)
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
