import { randomUUID } from 'node:crypto'
import { globalLogger } from '@lokalise/node-core'
import type { KafkaConsumerDependencies } from '@message-queue-toolkit/kafka'
import {
  AbstractKafkaConsumer,
  KafkaHandlerConfig,
  KafkaHandlerRoutingBuilder,
} from '@message-queue-toolkit/kafka'
import { config } from './config.ts'
import {
  DIRECT_EVENT_SCHEMA,
  DIRECT_ORDER_SCHEMA,
  DIRECT_TOPICS_CONFIG,
  type DirectEvent,
} from './direct-schemas.ts'
import type { MetricsCollector } from './metrics-collector.ts'

type ExecutionContext = {
  metrics: MetricsCollector
}

export class DirectConsumer extends AbstractKafkaConsumer<typeof DIRECT_TOPICS_CONFIG, ExecutionContext> {
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
          clientId: `direct-consumer-${randomUUID()}`,
        },
        groupId: `direct-load-test-${randomUUID()}`,
        batchProcessingEnabled: false,
        handlers: new KafkaHandlerRoutingBuilder<typeof DIRECT_TOPICS_CONFIG, ExecutionContext, false>()
          .addConfig(
            'direct-events',
            new KafkaHandlerConfig(DIRECT_EVENT_SCHEMA, (message, ctx) => {
              const value = message.value as DirectEvent
              const loadtestTs = typeof value.payload.loadtest_ts === 'number'
                ? value.payload.loadtest_ts
                : undefined
              ctx.metrics.recordConsumed('direct-events', loadtestTs)
            }),
          )
          .addConfig(
            'direct-orders',
            new KafkaHandlerConfig(DIRECT_ORDER_SCHEMA, (message, ctx) => {
              ctx.metrics.recordConsumed('direct-orders')
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
