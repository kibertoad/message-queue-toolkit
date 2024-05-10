/**
 * Check if the retry date is exceeded
 *
 * @param timestamp
 * @param maxRetryDuration max retry duration in seconds
 */
export const isRetryDateExceeded = (timestamp: Date, maxRetryDuration: number): boolean => {
  const lastRetryDate = new Date(timestamp.getTime() + maxRetryDuration * 1000)
  return new Date() > lastRetryDate
}
