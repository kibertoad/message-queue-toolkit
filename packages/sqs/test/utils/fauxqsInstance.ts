/** biome-ignore-all lint/suspicious/noConsole: test **/
import { type FauxqsServer, startFauxqs } from 'fauxqs'

const isLocalstack = process.env.QUEUE_BACKEND === 'localstack'
let server: FauxqsServer | undefined

export async function ensureFauxqsServer(): Promise<void> {
  if (isLocalstack) {
    console.log(`[fauxqs] skipping, QUEUE_BACKEND=${process.env.QUEUE_BACKEND}`)
    return
  }
  if (server) return
  server = await startFauxqs({ port: 4566, logger: false, host: 'localstack' })
  console.log('[fauxqs] server started on port 4566')
}

export function getFauxqsServer(): FauxqsServer | undefined {
  console.log(`[fauxqs] getFauxqsServer: ${server ? 'instance available' : 'undefined (localstack mode)'}`)
  return server
}
