import { InternalError } from '@lokalise/node-core'
import { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'
import { describe, expect, it } from 'vitest'
import { ZodError } from 'zod/v4'
import { AmqpConsumerErrorResolver } from './AmqpConsumerErrorResolver.ts'

describe('AmqpConsumerErrorResolver', () => {
  const resolver = new AmqpConsumerErrorResolver()

  it('returns MessageInvalidFormatError for SyntaxError', () => {
    const syntaxError = new SyntaxError('Unexpected token')

    const result = resolver.processError(syntaxError)

    expect(result).toBeInstanceOf(MessageInvalidFormatError)
    expect(result.message).toBe('Unexpected token')
  })

  it('returns MessageValidationError for ZodError', () => {
    const zodError = new ZodError([
      {
        code: 'invalid_type',
        expected: 'string',
        message: 'Expected string, received number',
        path: ['id'],
        input: 123,
      },
    ])

    const result = resolver.processError(zodError)

    expect(result).toBeInstanceOf(MessageValidationError)
    expect(result.details).toEqual({
      error: zodError.issues,
    })
  })

  it('returns InternalError for standardized errors', () => {
    // Create an error that matches the StandardizedError interface
    // StandardizedError requires: name, message, code, and Symbol.for('StandardizedErrorSymbol') = true
    const standardizedError = Object.assign(new Error('Custom error message'), {
      code: 'CUSTOM_ERROR',
      [Symbol.for('StandardizedErrorSymbol')]: true,
    })

    const result = resolver.processError(standardizedError)

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Custom error message')
    expect(result.errorCode).toBe('CUSTOM_ERROR')
  })

  it('returns InternalError with default message for unknown error types', () => {
    const unknownError = { someProperty: 'someValue' }

    const result = resolver.processError(unknownError)

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Error processing message')
    expect(result.errorCode).toBe('INTERNAL_ERROR')
  })

  it('returns InternalError with default message for null error', () => {
    const result = resolver.processError(null)

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Error processing message')
    expect(result.errorCode).toBe('INTERNAL_ERROR')
  })

  it('returns InternalError with default message for undefined error', () => {
    const result = resolver.processError(undefined)

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Error processing message')
    expect(result.errorCode).toBe('INTERNAL_ERROR')
  })

  it('returns InternalError with default message for string error', () => {
    const result = resolver.processError('some string error')

    expect(result).toBeInstanceOf(InternalError)
    expect(result.message).toBe('Error processing message')
    expect(result.errorCode).toBe('INTERNAL_ERROR')
  })
})
