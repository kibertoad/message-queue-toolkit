export const config = {
  kafka: {
    bootstrapBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    clientId: 'cdc-load-test',
  },
  crdb: {
    connectionString:
      process.env.CRDB_URL ?? 'postgresql://root@localhost:26257/loadtest?sslmode=disable',
    poolSize: 10,
  },
  reportIntervalMs: 5_000,
  drainTimeoutMs: 30_000,
} as const
