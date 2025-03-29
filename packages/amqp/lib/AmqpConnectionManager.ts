import type { ChannelModel } from 'amqplib'

import type { CommonLogger } from '@lokalise/node-core'
import type { AmqpConfig } from './amqpConnectionResolver'
import { resolveAmqpConnection } from './amqpConnectionResolver'

export type ConnectionReceiver = {
  receiveNewConnection(connection: ChannelModel): Promise<void>
  close(): Promise<void>
}

export class AmqpConnectionManager {
  private readonly config: AmqpConfig
  private readonly logger: CommonLogger
  private readonly connectionReceivers: ConnectionReceiver[]
  private connection?: ChannelModel
  public reconnectsActive: boolean

  public isReconnecting: boolean

  constructor(config: AmqpConfig, logger: CommonLogger) {
    this.config = config
    this.connectionReceivers = []
    this.reconnectsActive = true
    this.isReconnecting = false
    this.logger = logger
  }

  private async createConnection(): Promise<ChannelModel> {
    const connection = await resolveAmqpConnection(this.config)
    connection.on('error', (err) => {
      this.logger.error(`AmqpConnectionManager: Connection error: ${err.message}`)
      this.connection = undefined
      if (this.reconnectsActive && !this.isReconnecting) {
        void this.reconnect()
      }
    })
    connection.on('close', () => {
      if (this.reconnectsActive && !this.isReconnecting) {
        this.logger.error(`AmqpConnectionManager: Connection closed unexpectedly`)
        if (this.reconnectsActive) {
          void this.reconnect()
        }
      }
    })

    const promises: Promise<unknown>[] = []

    this.logger.info(
      `Propagating new connection across ${this.connectionReceivers.length} receivers`,
    )
    for (const receiver of this.connectionReceivers) {
      promises.push(receiver.receiveNewConnection(connection))
    }
    await Promise.all(promises)

    return connection
  }

  getConnectionSync() {
    return this.connection
  }

  async getConnection(): Promise<ChannelModel> {
    if (!this.connection) {
      this.connection = await this.createConnection()
    }
    return this.connection
  }

  async reconnect() {
    if (this.isReconnecting) {
      return
    }
    this.logger.info('AmqpConnectionManager: Start reconnecting')

    this.isReconnecting = true
    const oldConnection = this.connection
    this.connection = await this.createConnection()
    if (oldConnection) {
      try {
        await oldConnection.close()
      } catch {
        // this can fail
      }
    }
    this.isReconnecting = false

    this.logger.info('AmqpConnectionManager: Reconnect complete')
  }

  async init() {
    this.reconnectsActive = true
    await this.getConnection()
  }

  async close() {
    this.reconnectsActive = false

    for (const receiver of this.connectionReceivers) {
      await receiver.close()
    }

    try {
      await this.connection?.close()
    } catch {
      // it's OK
    }
    this.connection = undefined
  }

  subscribeConnectionReceiver(connectionReceiver: ConnectionReceiver) {
    this.connectionReceivers.push(connectionReceiver)
  }
}
