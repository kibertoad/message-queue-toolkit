import type { Connection } from 'amqplib'

export async function createSilentChannel(connection: Connection) {
  const channel = await connection.createChannel()
  channel.on('close', () => {
    // consume event
  })
  channel.on('error', (err) => {
    // consume event
  })

  return channel
}
