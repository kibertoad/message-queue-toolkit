import { FsReadableProvider } from '@lokalise/node-core'
import { JsonStreamStringify } from 'json-stream-stringify'
import { tmpNameSync } from 'tmp'

import type { PayloadSerializer } from './payloadStoreTypes'

export type TemporaryFilePathResolver = () => string
const defaultTemporaryFilePathResolver: TemporaryFilePathResolver = () => tmpNameSync()

export class JsonStringifyStreamSerializer implements PayloadSerializer {
  constructor(
    private readonly temporaryFilePathResolver: TemporaryFilePathResolver = defaultTemporaryFilePathResolver,
  ) {}

  async serialize(payload: unknown) {
    const fsReadableProvider = await FsReadableProvider.persistReadableToFs({
      sourceReadable: new JsonStreamStringify(payload),
      targetFile: this.temporaryFilePathResolver(),
    })
    return {
      value: await fsReadableProvider.createStream(),
      size: await fsReadableProvider.getContentLength(),
    }
  }
}

export const jsonStringifyStreamSerializer: PayloadSerializer = new JsonStringifyStreamSerializer()
