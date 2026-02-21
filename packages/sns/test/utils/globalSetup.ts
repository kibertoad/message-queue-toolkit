const isLocalstack = process.env.QUEUE_BACKEND === 'localstack'

let server: { stop: () => Promise<void> } | undefined

export async function setup() {
  if (!isLocalstack) {
    const { startFauxqs } = await import('fauxqs')
    server = await startFauxqs({ port: 4566, logger: false, host: 'localstack' })
  }
}

export async function teardown() {
  await server?.stop()
}
