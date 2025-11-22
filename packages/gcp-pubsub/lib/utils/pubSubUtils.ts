import type { PubSub } from '@google-cloud/pubsub'

export type PubSubConfig = {
  projectId: string
  emulatorHost?: string
}

export function createPubSubClient(config: PubSubConfig): PubSub {
  const { PubSub } = require('@google-cloud/pubsub')

  if (config.emulatorHost) {
    process.env.PUBSUB_EMULATOR_HOST = config.emulatorHost
  }

  return new PubSub({
    projectId: config.projectId,
  })
}
