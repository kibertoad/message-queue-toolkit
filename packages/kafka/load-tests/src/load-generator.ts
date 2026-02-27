import { setTimeout } from 'node:timers/promises'
import { CdcConsumer } from './cdc-consumer.ts'
import { config } from './config.ts'
import { CrdbClient } from './crdb-client.ts'
import { MetricsCollector } from './metrics-collector.ts'

export interface LoadTestOptions {
  rate: number
  duration: number
  batchSize: number
}

export async function runLoadTest(options: LoadTestOptions): Promise<void> {
  const { rate, duration, batchSize } = options

  console.log(`Starting CDC load test: ${rate} rows/sec, ${duration}s duration, batch=${batchSize}`)

  const metrics = new MetricsCollector()
  const crdb = new CrdbClient()
  const consumer = new CdcConsumer(metrics)

  // Start consumer
  console.log('Initializing Kafka consumer...')
  await consumer.init()
  console.log('Consumer ready.')

  // Periodic reporting
  const reportInterval = setInterval(() => metrics.report(), config.reportIntervalMs)

  // Generate load
  const totalRows = rate * duration
  const batchesPerSecond = rate / batchSize
  const delayMs = batchesPerSecond > 0 ? 1000 / batchesPerSecond : 1000

  console.log(
    `Generating ${totalRows.toLocaleString()} total rows (${batchesPerSecond.toFixed(1)} batches/sec, ${delayMs.toFixed(0)}ms between batches)`,
  )

  const loadStartTime = Date.now()
  let totalInserted = 0
  let inflight = 0
  const maxInflight = 10 // cap concurrent batch inserts

  while (totalInserted < totalRows) {
    // Wait if too many batches in flight
    while (inflight >= maxInflight) {
      await setTimeout(5)
    }

    const remaining = totalRows - totalInserted
    const currentBatch = Math.min(batchSize, remaining)
    const eventCount = Math.ceil(currentBatch / 2)
    const orderCount = currentBatch - eventCount

    totalInserted += currentBatch
    inflight++

    Promise.all([crdb.insertEvents(eventCount), crdb.insertOrders(orderCount)])
      .then(() => metrics.recordProduced(currentBatch))
      .catch((err) => console.error('Insert error:', err))
      .finally(() => {
        inflight--
      })

    // Throttle to target rate (0 = no throttle = max speed)
    if (rate > 0) {
      const elapsed = Date.now() - loadStartTime
      const expectedElapsed = (totalInserted / rate) * 1000
      const sleepMs = expectedElapsed - elapsed
      if (sleepMs > 0) {
        await setTimeout(sleepMs)
      }
    }
  }

  // Wait for all in-flight inserts to complete
  while (inflight > 0) {
    await setTimeout(10)
  }

  console.log(`\nLoad generation complete. ${totalInserted.toLocaleString()} rows inserted.`)
  console.log(`Waiting up to ${config.drainTimeoutMs / 1000}s for consumer to drain...`)

  // Wait for consumer to drain
  const drainStart = Date.now()
  while (metrics.backlog > 0 && Date.now() - drainStart < config.drainTimeoutMs) {
    await setTimeout(500)
  }

  // Final report
  clearInterval(reportInterval)
  metrics.printFinalReport()

  // Cleanup
  console.log('Shutting down...')
  await consumer.close()
  await crdb.close()
  console.log('Done.')
}
