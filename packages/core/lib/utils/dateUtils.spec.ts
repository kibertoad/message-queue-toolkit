import { isRetryDateExceeded } from './dateUtils'

describe('dateUtils', () => {
  describe('isRetryDateExceeded', () => {
    it('retry not exceeded', () => {
      const timestamp = new Date(new Date().getTime() - 59 * 1000)
      const maxRetryDuration = 60

      const result = isRetryDateExceeded(timestamp, maxRetryDuration)

      expect(result).toBe(false)
    })

    it('retry exceeded', () => {
      const timestamp = new Date(new Date().getTime() - 61 * 1000)
      const maxRetryDuration = 60

      const result = isRetryDateExceeded(timestamp, maxRetryDuration)

      expect(result).toBe(true)
    })
  })
})
