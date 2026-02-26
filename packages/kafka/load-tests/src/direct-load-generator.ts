import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import { config } from './config.ts'
import { DirectConsumer } from './direct-consumer.ts'
import { DirectPublisher } from './direct-publisher.ts'
import type { DirectEvent, DirectOrder } from './direct-schemas.ts'
import { MetricsCollector } from './metrics-collector.ts'

export interface LoadTestOptions {
  rate: number
  duration: number
  batchSize: number
}

function generateEvent(index: number): DirectEvent {
  return {
    id: randomUUID(),
    event_type: `load_test_${index % 5}`,
    payload: { loadtest_ts: Date.now(), index, data: `event-payload-${index}` },
    created_at: new Date().toISOString(),
  }
}

function generateOrder(index: number): DirectOrder {
  return {
    id: randomUUID(),
    customer_id: `customer-${(index % 100).toString().padStart(3, '0')}`,
    amount: (Math.random() * 1000).toFixed(2),
    status: ['pending', 'confirmed', 'shipped', 'delivered'][index % 4]!,
    created_at: new Date().toISOString(),
  }
}

export async function runDirectLoadTest(options: LoadTestOptions): Promise<void> {
  const { rate, duration, batchSize } = options

  console.log(`Starting direct Kafka load test: ${rate} msgs/sec, ${duration}s duration, batch=${batchSize}`)

  const metrics = new MetricsCollector()
  const publisher = new DirectPublisher()
  const consumer = new DirectConsumer(metrics)

  // Start consumer and publisher
  console.log('Initializing Kafka consumer and publisher...')
  await Promise.all([consumer.init(), publisher.init()])
  console.log('Consumer and publisher ready.')

  // Periodic reporting
  const reportInterval = setInterval(() => metrics.report(), config.reportIntervalMs)

  // Generate load
  const totalMessages = rate * duration

  console.log(
    `Publishing ${totalMessages.toLocaleString()} total messages at ${rate}/sec`,
  )

  const loadStartTime = Date.now()
  let totalPublished = 0

  while (totalPublished < totalMessages) {
    const remaining = totalMessages - totalPublished
    const currentBatch = Math.min(batchSize, remaining)

    // Split evenly between events and orders
    const eventCount = Math.ceil(currentBatch / 2)
    const orderCount = currentBatch - eventCount

    try {
      const promises: Promise<void>[] = []
      for (let i = 0; i < eventCount; i++) {
        promises.push(publisher.publish('direct-events', generateEvent(totalPublished + i)))
      }
      for (let i = 0; i < orderCount; i++) {
        promises.push(publisher.publish('direct-orders', generateOrder(totalPublished + eventCount + i)))
      }
      await Promise.all(promises)
      totalPublished += currentBatch
      metrics.recordProduced(currentBatch)
    } catch (err) {
      console.error('Publish error:', err)
    }

    // Throttle to target rate
    const elapsed = Date.now() - loadStartTime
    const expectedElapsed = (totalPublished / rate) * 1000
    const sleepMs = expectedElapsed - elapsed
    if (sleepMs > 0) {
      await setTimeout(sleepMs)
    }
  }

  console.log(`\nPublishing complete. ${totalPublished.toLocaleString()} messages published.`)
  console.log(`Waiting up to ${config.drainTimeoutMs / 1000}s for consumer to drain...`)

  // Wait for consumer to drain
  const drainStart = Date.now()
  while (
    metrics.backlog > 0 &&
    Date.now() - drainStart < config.drainTimeoutMs
  ) {
    await setTimeout(500)
  }

  // Final report
  clearInterval(reportInterval)
  metrics.printFinalReport()

  // Cleanup
  console.log('Shutting down...')
  await Promise.all([consumer.close(), publisher.close()])
  console.log('Done.')
}
