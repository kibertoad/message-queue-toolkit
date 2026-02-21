/** biome-ignore-all lint/suspicious/noConsole: test **/
import { type FauxqsServer, startFauxqs } from 'fauxqs'

const isLocalstack = process.env.QUEUE_BACKEND === 'localstack'
const LOCALSTACK_PORT = 4566
const LOCALSTACK_HOST = 'localstack'
const FAUXQS_PORT = 4567
const FAUXQS_HOST = 'localstack'

let server: FauxqsServer | undefined

export function getPort(): number {
  return isLocalstack ? LOCALSTACK_PORT : FAUXQS_PORT
}

export function getHost(): string {
  return isLocalstack ? LOCALSTACK_HOST : FAUXQS_HOST
}

export async function ensureFauxqsServer(): Promise<void> {
  if (isLocalstack) {
    console.log(`[fauxqs] skipping, QUEUE_BACKEND=${process.env.QUEUE_BACKEND}`)
    return
  }
  if (server) return
  server = await startFauxqs({ port: FAUXQS_PORT, logger: false, host: 'localstack' })
  console.log(`[fauxqs] server started on port ${FAUXQS_PORT}`)
}

export function getFauxqsServer(): FauxqsServer | undefined {
  console.log(
    `[fauxqs] getFauxqsServer: ${server ? 'instance available' : 'undefined (localstack mode)'}`,
  )
  return server
}
