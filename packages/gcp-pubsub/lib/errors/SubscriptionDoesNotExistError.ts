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
    error.name === 'SubscriptionDoesNotExistError'
  )
}
