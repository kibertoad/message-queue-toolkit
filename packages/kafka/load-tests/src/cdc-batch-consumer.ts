import { randomUUID } from 'node:crypto'
import { globalLogger } from '@lokalise/node-core'
import type { KafkaConsumerDependencies } from '@message-queue-toolkit/kafka'
import {
  AbstractKafkaConsumer,
  KafkaHandlerConfig,
  KafkaHandlerRoutingBuilder,
} from '@message-queue-toolkit/kafka'
import {
  CDC_EVENT_SCHEMA,
  CDC_ORDER_SCHEMA,
  type CDC_TOPICS_CONFIG,
  type CdcEvent,
  type CdcOrder,
} from './cdc-schemas.ts'
import { config } from './config.ts'
import type { MetricsCollector } from './metrics-collector.ts'

type ExecutionContext = {
  metrics: MetricsCollector
}

export class CdcBatchConsumer extends AbstractKafkaConsumer<
  typeof CDC_TOPICS_CONFIG,
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
          clientId: `cdc-batch-consumer-${randomUUID()}`,
        },
        groupId: `cdc-batch-load-test-${randomUUID()}`,
        batchProcessingEnabled: true,
        batchProcessingOptions: {
          batchSize,
          timeoutMilliseconds: timeoutMs,
        },
        handlers: new KafkaHandlerRoutingBuilder<typeof CDC_TOPICS_CONFIG, ExecutionContext, true>()
          .addConfig(
            'events',
            new KafkaHandlerConfig(CDC_EVENT_SCHEMA, (messages, ctx) => {
              console.log(
                `[${new Date().toISOString()}] [batch-handler] events batch received: ${messages.length} messages`,
              )
              for (const message of messages) {
                const value = message.value as CdcEvent
                if (!value.after) continue // skip resolved heartbeats
                let loadtestTs: number | undefined
                const payload = value.after.payload as Record<string, unknown>
                if (typeof payload.loadtest_ts === 'number') {
                  loadtestTs = payload.loadtest_ts
                }
                ctx.metrics.recordConsumed('events', loadtestTs)
              }
            }),
          )
          .addConfig(
            'orders',
            new KafkaHandlerConfig(CDC_ORDER_SCHEMA, (messages, ctx) => {
              console.log(
                `[${new Date().toISOString()}] [batch-handler] orders batch received: ${messages.length} messages`,
              )
              for (const message of messages) {
                const value = message.value as CdcOrder
                if (!value.after) continue // skip resolved heartbeats
                ctx.metrics.recordConsumed('orders')
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
