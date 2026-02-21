/** biome-ignore-all lint/suspicious/noConsole: test **/
import { type FauxqsServer, startFauxqs } from 'fauxqs'

let server: FauxqsServer | undefined

export async function ensureFauxqsServer(): Promise<void> {
  if (server) return
  server = await startFauxqs({ port: 0, logger: false, host: 'localstack' })
  console.log(`[fauxqs] server started on port ${server.port}`)
}

export function getFauxqsServer(): FauxqsServer | undefined {
  return server
}

export function getFauxqsPort(): number {
  if (!server) throw new Error('fauxqs server not started — call ensureFauxqsServer() first')
  return server.port
}
