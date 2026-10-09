import { setTimeout } from 'node:timers/promises'
import { describe, expect, it } from 'vitest'

import { waitAndRetry } from '../../lib/utils/waitUtils.ts'

describe('waitUtils', () => {
  describe('waitAndRetry', () => {
    it('resolves with the predicate result once it is truthy', async () => {
      let calls = 0

      const result = await waitAndRetry(() => {
        calls++
        return calls === 3 ? 'done' : undefined
      }, 1)

      expect(result).toBe('done')
      expect(calls).toBe(3)
    })

    it('stops polling after the retries are exhausted', async () => {
      let calls = 0

      const result = await waitAndRetry(
        () => {
          calls++
          return false
        },
        1,
        3,
      )

      expect(result).toBe(false)
      // 4 checks that count as retries, plus the final check whose result is returned
      expect(calls).toBe(5)

      await setTimeout(50)
      expect(calls).toBe(5)
    })

    it('rejects when the final check after the retries are exhausted rejects', async () => {
      let calls = 0

      const promise = waitAndRetry(
        () => {
          calls++
          return calls > 4 ? Promise.reject(new Error('unreachable')) : Promise.resolve(false)
        },
        1,
        3,
      )

      await expect(promise).rejects.toThrow('unreachable')
      await setTimeout(50)
      expect(calls).toBe(5)
    })
  })
})
