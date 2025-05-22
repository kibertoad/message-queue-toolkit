import { describe, expect, it } from 'vitest'
import { z } from 'zod/v3'

import { toDatePreprocessor } from './toDateProcessor.ts'

describe('toDatePreprocessor', () => {
  it('converts valid strings to date', () => {
    const SCHEMA = z.object({
      createdAt: z.preprocess(toDatePreprocessor, z.date()),
      updatedAt: z.preprocess(toDatePreprocessor, z.date()),
    })

    const result = SCHEMA.parse({
      createdAt: '2016-01-01',
      updatedAt: '2022-01-12T00:00:00.000Z',
    })

    expect(result).toEqual({
      createdAt: new Date('2016-01-01'),
      updatedAt: new Date('2022-01-12T00:00:00.000Z'),
    })
  })

  it('converts valid numbers to date', () => {
    const SCHEMA = z.object({
      createdAt: z.preprocess(toDatePreprocessor, z.date()),
      updatedAt: z.preprocess(toDatePreprocessor, z.date()),
    })

    const result = SCHEMA.parse({
      createdAt: 166,
      updatedAt: 99999,
    })

    expect(result).toEqual({
      createdAt: new Date(166),
      updatedAt: new Date(99999),
    })
  })

  it('does not convert function input', () => {
    const SCHEMA = z.object({
      createdAt: z.preprocess(toDatePreprocessor, z.date()),
    })

    expect(() =>
      SCHEMA.parse({
        createdAt: (x: string) => x,
      }),
    ).toThrow(/Expected date, received function/)
  })
})
