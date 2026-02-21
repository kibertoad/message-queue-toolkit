import { type FauxqsServer, startFauxqs } from 'fauxqs'

const isLocalstack = process.env.QUEUE_BACKEND === 'localstack'
let server: FauxqsServer | undefined

export async function ensureFauxqsServer(): Promise<void> {
  if (isLocalstack || server) return
  server = await startFauxqs({ port: 4566, logger: false, host: 'localstack' })
}

export function getFauxqsServer(): FauxqsServer | undefined {
  return server
}
