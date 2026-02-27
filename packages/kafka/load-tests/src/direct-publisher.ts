import { randomUUID } from 'node:crypto'
import { globalLogger } from '@lokalise/node-core'
import { AbstractKafkaPublisher, type KafkaDependencies } from '@message-queue-toolkit/kafka'
import { config } from './config.ts'
import { DIRECT_TOPICS_CONFIG } from './direct-schemas.ts'

export class DirectPublisher extends AbstractKafkaPublisher<typeof DIRECT_TOPICS_CONFIG> {
  constructor() {
    const deps: KafkaDependencies = {
      logger: globalLogger,
      errorReporter: { report: () => {} },
    }

    super(deps, {
      kafka: {
        bootstrapBrokers: config.kafka.bootstrapBrokers,
        clientId: `direct-publisher-${randomUUID()}`,
      },
      topicsConfig: DIRECT_TOPICS_CONFIG,
      autocreateTopics: true,
      logMessages: false,
      handlerSpy: false,
    })
  }
}
