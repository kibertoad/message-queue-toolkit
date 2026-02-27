import { setTimeout } from 'node:timers/promises'
import { CdcBatchConsumer } from './cdc-batch-consumer.ts'
import { config } from './config.ts'
import { CrdbClient } from './crdb-client.ts'
import { MetricsCollector } from './metrics-collector.ts'

export interface BatchLoadTestOptions {
  rate: number
  duration: number
  batchSize: number
  consumerBatchSize: number
  consumerBatchTimeoutMs: number
}

export async function runBatchLoadTest(options: BatchLoadTestOptions): Promise<void> {
  const { rate, duration, batchSize, consumerBatchSize, consumerBatchTimeoutMs } = options

  console.log(
    `Starting CDC batch load test: ${rate} rows/sec, ${duration}s duration, insert batch=${batchSize}, consumer batch=${consumerBatchSize}, timeout=${consumerBatchTimeoutMs}ms`,
  )

  const metrics = new MetricsCollector()
  const crdb = new CrdbClient()
  const consumer = new CdcBatchConsumer(metrics, consumerBatchSize, consumerBatchTimeoutMs)

  console.log('Initializing Kafka batch consumer...')
  await consumer.init()
  console.log('Batch consumer ready.')

  const reportInterval = setInterval(() => metrics.report(), config.reportIntervalMs)

  const totalRows = rate * duration

  console.log(`Generating ${totalRows.toLocaleString()} total rows`)

  const loadStartTime = Date.now()
  let totalInserted = 0
  let inflight = 0
  const maxInflight = 10

  while (totalInserted < totalRows) {
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

    if (rate > 0) {
      const elapsed = Date.now() - loadStartTime
      const expectedElapsed = (totalInserted / rate) * 1000
      const sleepMs = expectedElapsed - elapsed
      if (sleepMs > 0) {
        await setTimeout(sleepMs)
      }
    }
  }

  while (inflight > 0) {
    await setTimeout(10)
  }

  console.log(`\nLoad generation complete. ${totalInserted.toLocaleString()} rows inserted.`)
  console.log(`Waiting up to ${config.drainTimeoutMs / 1000}s for consumer to drain...`)

  const drainStart = Date.now()
  while (metrics.backlog > 0 && Date.now() - drainStart < config.drainTimeoutMs) {
    await setTimeout(500)
  }

  clearInterval(reportInterval)
  metrics.printFinalReport()

  console.log('Shutting down...')
  await consumer.close()
  await crdb.close()
  console.log('Done.')
}
