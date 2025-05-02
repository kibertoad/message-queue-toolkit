import { setTimeout } from 'node:timers/promises'

import { globalLogger } from '@lokalise/node-core'
import { type ChannelModel, connect } from 'amqplib'

const MAX_RETRY_ATTEMPTS = 10

export type AmqpConfig = {
  hostname: string
  port: number
  username: string
  password: string
  vhost: string
  useTls: boolean
}

export async function resolveAmqpConnection(config: AmqpConfig): Promise<ChannelModel> {
  const protocol = config.useTls ? 'amqps' : 'amqp'
  let counter = 0
  while (true) {
    const url = `${protocol}://${config.username}:${config.password}@${config.hostname}:${config.port}/${config.vhost}`

    // exponential backoff -> (2 ^ (attempt)) * delay
    // delay = 1 second
    const retryTime = Math.pow(2, counter + 1) * 1000

    try {
      const connection = await connect(url)
      return connection
    } catch (_e) {
      globalLogger.error(
        `Failed to connect to AMQP broker at ${config.hostname}:${config.port}. Retrying in ${
          retryTime / 1000
        } seconds...`,
      )
    }
    await setTimeout(retryTime)
    counter++

    if (counter > MAX_RETRY_ATTEMPTS) {
      throw new Error('Failed to resolve AMQP connection')
    }
  }
}
