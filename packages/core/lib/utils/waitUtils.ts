export const waitAndRetry = <T>(
  predicateFn: () => T,
  sleepTime = 20,
  maxRetryCount = 15,
): Promise<T> => {
  return new Promise((resolve, reject) => {
    let retryCount = 0
    function performCheck() {
      if (maxRetryCount !== 0 && retryCount > maxRetryCount) {
        resolve(predicateFn())
      }
      Promise.resolve()
        .then(() => {
          return predicateFn()
        })
        .then((result) => {
          if (result) {
            resolve(result)
          } else {
            retryCount++
            setTimeout(performCheck, sleepTime)
          }
        })
        .catch((err) => {
          reject(err)
        })
    }

    performCheck()
  })
}
