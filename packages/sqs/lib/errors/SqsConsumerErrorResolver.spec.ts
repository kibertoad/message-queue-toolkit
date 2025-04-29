import { describe, expect, it } from 'vitest'
import { SqsConsumerErrorResolver } from './SqsConsumerErrorResolver.ts'

describe('SqsConsumerErrorResolver', () => {
  const resolver = new SqsConsumerErrorResolver()
  it('Resolves error from standardized error', () => {
    const error = resolver.processError({
      message: 'someError',
      code: 'ERROR_CODE',
    })

    expect(error).toMatchObject({
      message: 'someError',
      errorCode: 'ERROR_CODE',
    })
  })
})
