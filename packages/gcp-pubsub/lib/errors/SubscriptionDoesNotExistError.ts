export class SubscriptionDoesNotExistError extends Error {
  public readonly subscriptionName: string

  constructor(subscriptionName: string) {
    super(`Subscription ${subscriptionName} does not exist`)
    this.name = 'SubscriptionDoesNotExistError'
    this.subscriptionName = subscriptionName
  }
}

export function isSubscriptionDoesNotExistError(
  error: unknown,
): error is SubscriptionDoesNotExistError {
  return (
    typeof error === 'object' &&
    error !== null &&
    'name' in error &&
    'message' in error &&
    'subscriptionName' in error &&
    error.name === 'SubscriptionDoesNotExistError' &&
    typeof error.message === 'string' &&
    typeof error.subscriptionName === 'string'
  )
}
