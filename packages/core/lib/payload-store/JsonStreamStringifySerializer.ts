import { FsReadableProvider } from '@lokalise/node-core'
import { JsonStreamStringify } from 'json-stream-stringify'
import { tmpNameSync } from 'tmp'

import type { PayloadSerializer } from './payloadStoreTypes'

export type TemporaryFilePathResolver = () => string
export const defaultTemporaryFilePathResolver: TemporaryFilePathResolver = () => tmpNameSync()

export class JsonStreamStringifySerializer implements PayloadSerializer {
  constructor(
    private readonly temporaryFilePathResolver: TemporaryFilePathResolver = defaultTemporaryFilePathResolver,
  ) {}

  async serialize(payload: unknown) {
    const fsReadableProvider = await FsReadableProvider.persistReadableToFs({
      sourceReadable: new JsonStreamStringify(payload),
      targetFile: this.temporaryFilePathResolver(),
    })

    const fsBasedStream = await fsReadableProvider.createStream()
    // When stream is read to the end, we can destroy the file
    fsBasedStream.on('end', () => {
      void fsReadableProvider.destroy()
    })

    return {
      value: fsBasedStream,
      size: await fsReadableProvider.getContentLength(),
    }
  }
}

export const jsonStreamStringifySerializer: PayloadSerializer = new JsonStreamStringifySerializer()
