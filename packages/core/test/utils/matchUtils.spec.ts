import { describe } from 'vitest'

import { isShallowSubset } from '../../lib/utils/matchUtils'

describe('matchUtils', () => {
  describe('isShallowSubset', () => {
    it('returns true for exact match', () => {
      const set1 = { a: 'a', b: 1 }
      const set2 = { a: 'a', b: 1 }

      expect(isShallowSubset(set1, set2)).toBe(true)
    })

    it('returns true for same fields with different values', () => {
      const set1 = { a: 'a', b: 1 }
      const set2 = { a: 'a', b: 2 }

      expect(isShallowSubset(set1, set2)).toBe(false)
    })

    it('returns true for a subset', () => {
      const set1 = { a: 'a' }
      const set2 = { a: 'a', b: 2 }

      expect(isShallowSubset(set1, set2)).toBe(true)
    })

    it('returns false for a superset', () => {
      const set1 = { a: 'a', b: 2 }
      const set2 = { a: 'a' }

      expect(isShallowSubset(set1, set2)).toBe(false)
    })

    it('returns true for empty set', () => {
      const set1 = {}
      const set2 = { a: 'a', b: 2 }

      expect(isShallowSubset(set1, set2)).toBe(true)
    })

    it('returns true for undefined', () => {
      const set1 = undefined
      const set2 = { a: 'a', b: 2 }

      expect(isShallowSubset(set1, set2)).toBe(true)
    })
  })
})
