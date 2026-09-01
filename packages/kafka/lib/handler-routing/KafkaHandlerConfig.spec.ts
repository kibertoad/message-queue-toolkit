import { precompileSchema } from '@message-queue-toolkit/core'
import z from 'zod/v4'
import { KafkaHandlerConfig } from './KafkaHandlerConfig.ts'

const MESSAGE_SCHEMA = z.object({ type: z.literal('create'), id: z.string() })

describe('KafkaHandlerConfig', () => {
  it('precompiles the schema it is given', () => {
    const config = new KafkaHandlerConfig(MESSAGE_SCHEMA, () => {})

    expect(config.schema).toBe(precompileSchema(MESSAGE_SCHEMA))
    expect(config.schema).not.toBe(MESSAGE_SCHEMA)
  })

  it('keeps parsing behaviour intact', () => {
    const config = new KafkaHandlerConfig(MESSAGE_SCHEMA, () => {})

    expect(config.schema.parse({ type: 'create', id: '1' })).toEqual({ type: 'create', id: '1' })
    expect(config.schema.safeParse({ type: 'create' }).success).toBe(false)
  })
})
