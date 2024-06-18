import * as fs from 'node:fs/promises'

import { describe } from 'vitest'

import {
  defaultTemporaryFilePathResolver,
  JsonStreamStringifySerializer,
} from '../../lib/payload-store/JsonStreamStringifySerializer'
import { streamToString } from '../utils/streamUtils'

describe('JsonStreamStringifySerializer', () => {
  it('serializes object to stream', async () => {
    const payload = { key: 'value' }
    const serialized = await new JsonStreamStringifySerializer().serialize(payload)

    expect(serialized.size).toBe(JSON.stringify(payload).length)
    await expect(streamToString(serialized.value)).resolves.toEqual(JSON.stringify(payload))
  })

  it('removes temporary file when destructor is called', async () => {
    const temporaryFilePath = defaultTemporaryFilePathResolver()
    const temporaryFilePathResolver = vi.fn(() => temporaryFilePath)

    // Check that file does not exist
    await expect(fs.access(temporaryFilePath)).rejects.toThrow()

    const payload = { key: 'value' }
    const serializer = new JsonStreamStringifySerializer(temporaryFilePathResolver)
    const serialized = await serializer.serialize(payload)

    // Check that file exists
    await expect(fs.access(temporaryFilePath)).resolves.toBeUndefined()

    // Destroy the payload
    await serialized.destroy()

    // Check that file does not exist
    await expect(fs.access(temporaryFilePath)).rejects.toThrow()
  })
})
