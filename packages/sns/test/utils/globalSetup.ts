import { startFauxqs } from 'fauxqs'

let server: Awaited<ReturnType<typeof startFauxqs>>

export async function setup() {
  server = await startFauxqs({ port: 4566, logger: false, host: 'localstack' })
  console.log(`fauxqs started on port ${server.port}`)
}

export async function teardown() {
  await server.stop()
  console.log('fauxqs stopped')
}
