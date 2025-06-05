import { randomUUID } from 'node:crypto'
import type { KafkaConfig } from '../../lib/index.js'

export const TEST_KAFKA_CONFIG: KafkaConfig = {
  bootstrapBrokers: ['localhost:9092'],
  clientId: randomUUID(),
}
