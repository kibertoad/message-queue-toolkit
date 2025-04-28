import type { AmqpConfig } from '../../lib/amqpConnectionResolver.ts'

export const TEST_AMQP_CONFIG: AmqpConfig = {
  vhost: '',
  hostname: 'localhost',
  username: 'guest',
  password: 'guest',
  port: 5672,
  useTls: false,
}
