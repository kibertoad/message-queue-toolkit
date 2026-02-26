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
  type DIRECT_TOPICS_CONFIG,
  type DirectEvent,
} from './direct-schemas.ts'
import type { MetricsCollector } from './metrics-collector.ts'

type ExecutionContext = {
  metrics: MetricsCollector
}

export class DirectBatchConsumer extends AbstractKafkaConsumer<
  typeof DIRECT_TOPICS_CONFIG,
  ExecutionContext,
  true
> {
  constructor(metrics: MetricsCollector, batchSize = 50, timeoutMs = 200) {
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
          clientId: `direct-batch-consumer-${randomUUID()}`,
        },
        groupId: `direct-batch-load-test-${randomUUID()}`,
        batchProcessingEnabled: true,
        batchProcessingOptions: {
          batchSize,
          timeoutMilliseconds: timeoutMs,
        },
        handlers: new KafkaHandlerRoutingBuilder<
          typeof DIRECT_TOPICS_CONFIG,
          ExecutionContext,
          true
        >()
          .addConfig(
            'direct-events',
            new KafkaHandlerConfig(DIRECT_EVENT_SCHEMA, (messages, ctx) => {
              console.log(
                `[${new Date().toISOString()}] [batch-handler] direct-events batch received: ${messages.length} messages`,
              )
              for (const message of messages) {
                const value = message.value as DirectEvent
                const loadtestTs =
                  typeof value.payload.loadtest_ts === 'number'
                    ? value.payload.loadtest_ts
                    : undefined
                ctx.metrics.recordConsumed('direct-events', loadtestTs)
              }
            }),
          )
          .addConfig(
            'direct-orders',
            new KafkaHandlerConfig(DIRECT_ORDER_SCHEMA, (messages, ctx) => {
              console.log(
                `[${new Date().toISOString()}] [batch-handler] direct-orders batch received: ${messages.length} messages`,
              )
              for (const message of messages) {
                ctx.metrics.recordConsumed('direct-orders')
              }
            }),
          )
          .build(),
        autocreateTopics: true,
        logMessages: true,
        handlerSpy: false,
        maxWaitTime: 5,
      },
      { metrics },
    )
  }
}
