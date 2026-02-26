import pg from 'pg'
import { config } from './config.ts'

export class CrdbClient {
  private readonly pool: pg.Pool

  constructor() {
    this.pool = new pg.Pool({
      connectionString: config.crdb.connectionString,
      max: config.crdb.poolSize,
    })
  }

  async insertEvents(count: number): Promise<void> {
    if (count === 0) return

    const values: unknown[] = []
    const placeholders: string[] = []

    for (let i = 0; i < count; i++) {
      const offset = i * 2
      placeholders.push(`(gen_random_uuid(), $${offset + 1}::STRING, $${offset + 2}::JSONB, now())`)
      values.push(
        `load_test_${i % 5}`,
        JSON.stringify({ loadtest_ts: Date.now(), index: i, data: `event-payload-${i}` }),
      )
    }

    await this.pool.query(
      `INSERT INTO events (id, event_type, payload, created_at) VALUES ${placeholders.join(', ')}`,
      values,
    )
  }

  async insertOrders(count: number): Promise<void> {
    if (count === 0) return

    const values: unknown[] = []
    const placeholders: string[] = []

    for (let i = 0; i < count; i++) {
      const offset = i * 3
      placeholders.push(`(gen_random_uuid(), $${offset + 1}::STRING, $${offset + 2}::DECIMAL, $${offset + 3}::STRING, now())`)
      values.push(
        `customer-${(i % 100).toString().padStart(3, '0')}`,
        (Math.random() * 1000).toFixed(2),
        ['pending', 'confirmed', 'shipped', 'delivered'][i % 4],
      )
    }

    await this.pool.query(
      `INSERT INTO orders (id, customer_id, amount, status, created_at) VALUES ${placeholders.join(', ')}`,
      values,
    )
  }

  async close(): Promise<void> {
    await this.pool.end()
  }
}
