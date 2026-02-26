import { randomUUID } from 'node:crypto'
import { globalLogger } from '@lokalise/node-core'
import type { KafkaConsumerDependencies } from '@message-queue-toolkit/kafka'
import {
  AbstractKafkaConsumer,
  KafkaHandlerConfig,
  KafkaHandlerRoutingBuilder,
} from '@message-queue-toolkit/kafka'
import { config } from './config.ts'
import { CDC_EVENT_SCHEMA, CDC_ORDER_SCHEMA, CDC_TOPICS_CONFIG, type CdcEvent, type CdcOrder } from './cdc-schemas.ts'
import type { MetricsCollector } from './metrics-collector.ts'

type ExecutionContext = {
  metrics: MetricsCollector
}

export class CdcConsumer extends AbstractKafkaConsumer<typeof CDC_TOPICS_CONFIG, ExecutionContext> {
  constructor(metrics: MetricsCollector) {
    const deps: KafkaConsumerDependencies = {
      logger: globalLogger,
      errorReporter: { report: () => {} },
      transactionObservabilityManager: {
        start: () => {},
        stop: () => {},
        startWithGroup: () => {},
        addCustomAttributes: () => {},
      },
    }

    super(
      deps,
      {
        kafka: {
          bootstrapBrokers: config.kafka.bootstrapBrokers,
          clientId: `cdc-consumer-${randomUUID()}`,
        },
        groupId: `cdc-load-test-${randomUUID()}`,
        batchProcessingEnabled: false,
        handlers: new KafkaHandlerRoutingBuilder<typeof CDC_TOPICS_CONFIG, ExecutionContext, false>()
          .addConfig(
            'events',
            new KafkaHandlerConfig(CDC_EVENT_SCHEMA, (message, ctx) => {
              const value = message.value as CdcEvent
              if (!value.after) return // skip resolved heartbeats
              let loadtestTs: number | undefined
              const payload = value.after.payload as Record<string, unknown>
              if (typeof payload.loadtest_ts === 'number') {
                loadtestTs = payload.loadtest_ts
              }
              ctx.metrics.recordConsumed('events', loadtestTs)
            }),
          )
          .addConfig(
            'orders',
            new KafkaHandlerConfig(CDC_ORDER_SCHEMA, (message, ctx) => {
              const value = message.value as CdcOrder
              if (!value.after) return // skip resolved heartbeats
              ctx.metrics.recordConsumed('orders')
            }),
          )
          .build(),
        autocreateTopics: true,
        logMessages: false,
        handlerSpy: false,
        maxWaitTime: 5,
      },
      { metrics },
    )
  }
}
