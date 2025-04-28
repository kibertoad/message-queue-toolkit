import { FsReadableProvider } from '@lokalise/node-core'
import { JsonStreamStringify } from 'json-stream-stringify'
import { tmpNameSync } from 'tmp'

import type { PayloadSerializer } from './payloadStoreTypes.ts'

export type TemporaryFilePathResolver = () => string
export const defaultTemporaryFilePathResolver: TemporaryFilePathResolver = () => tmpNameSync()

export class JsonStreamStringifySerializer implements PayloadSerializer {
  private readonly temporaryFilePathResolver: TemporaryFilePathResolver

  constructor(
    temporaryFilePathResolver: TemporaryFilePathResolver = defaultTemporaryFilePathResolver,
  ) {
    this.temporaryFilePathResolver = temporaryFilePathResolver
  }

  async serialize(payload: unknown) {
    const fsReadableProvider = await FsReadableProvider.persistReadableToFs({
      sourceReadable: new JsonStreamStringify(payload),
      targetFile: this.temporaryFilePathResolver(),
    })

    return {
      value: await fsReadableProvider.createStream(),
      size: await fsReadableProvider.getContentLength(),
      destroy: async () => {
        await fsReadableProvider.destroy()
      },
    }
  }
}

export const jsonStreamStringifySerializer: PayloadSerializer = new JsonStreamStringifySerializer()
