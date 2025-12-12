import { InternalError } from '@lokalise/node-core'
import { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'
import { describe, expect, it } from 'vitest'
import { ZodError } from 'zod/v4'
import { PubSubConsumerErrorResolver } from '../../lib/errors/PubSubConsumerErrorResolver.ts'

describe('PubSubConsumerErrorResolver', () => {
  const errorResolver = new PubSubConsumerErrorResolver()

  it('returns MessageInvalidFormatError for SyntaxError', () => {
    const syntaxError = new SyntaxError('Unexpected token')

    const result = errorResolver.processError(syntaxError)

    expect(result).toBeInstanceOf(MessageInvalidFormatError)
    expect(result.message).toBe('Unexpected token')
  })

  it('returns MessageValidationError for ZodError', () => {
    const zodError = new ZodError([
      {
        code: 'invalid_type',
        expected: 'string',
        path: ['field'],
        message: 'Expected string, received number',
      },
    ])

    const result = errorResolver.processError(zodError)

    expect(result).toBeInstanceOf(MessageValidationError)
    expect(result.message).toContain('invalid_type')
  })

  it('returns InternalError for StandardizedError', () => {
    // Create an error that matches the StandardizedError interface
    // StandardizedError requires: name, message, code, and Symbol.for('StandardizedErrorSymbol') = true
    const standardizedError = Object.assign(new Error('Standardized error'), {
      code: 'CUSTOM_CODE',
      [Symbol.for('StandardizedErrorSymbol')]: true,
    })

    const result = errorResolver.processError(standardizedError)

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Standardized error')
    expect(result.errorCode).toBe('CUSTOM_CODE')
  })

  it('returns InternalError for unknown errors', () => {
    const unknownError = { message: 'Unknown error' }

    const result = errorResolver.processError(unknownError)

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Error processing message')
    expect(result.errorCode).toBe('INTERNAL_ERROR')
  })

  it('returns InternalError for regular Error', () => {
    const regularError = new Error('Regular error')

    const result = errorResolver.processError(regularError)

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Error processing message')
    expect(result.errorCode).toBe('INTERNAL_ERROR')
  })
})
