import * as fs from 'node:fs/promises'

import { describe } from 'vitest'

import {
  defaultTemporaryFilePathResolver,
  JsonStreamStringifySerializer,
} from '../../lib/payload-store/JsonStreamStringifySerializer'
import { waitAndRetry } from '../../lib/utils/waitUtils'
import { streamToString } from '../utils/streamUtils'



describe('JsonStreamStringifySerializer', () => {
  it('should serialize object to stream', async () => {
    const payload = { key: 'value' }
    const serialized = await new JsonStreamStringifySerializer().serialize(payload)

    expect(serialized.size).toBe(JSON.stringify(payload).length)
    await expect(streamToString(serialized.value)).resolves.toEqual(JSON.stringify(payload))
  })

  it('should remove temporary file after stream is read to the end', async () => {
    const temporaryFilePath = defaultTemporaryFilePathResolver()
    const temporaryFilePathResolver = vi.fn(() => temporaryFilePath)

    // Check that file does not exist
    await expect(fs.access(temporaryFilePath)).rejects.toThrow()

    const payload = { key: 'value' }
    const serializer = new JsonStreamStringifySerializer(temporaryFilePathResolver)
    const serialized = await serializer.serialize(payload)

    // Check that file exists
    await expect(fs.access(temporaryFilePath)).resolves.toBeUndefined()
    // Consume the stream
    await expect(streamToString(serialized.value)).resolves.toEqual(JSON.stringify(payload))

    // Give it some time to remove the file
    await waitAndRetry(async () => {
      try {
        await fs.access(temporaryFilePath)
        return false
      } catch {
        return true
      }
    })

    // Check that file does not exist
    await expect(fs.access(temporaryFilePath)).rejects.toThrow()
  })
})
