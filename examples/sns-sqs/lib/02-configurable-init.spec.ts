import { SnsConsumerErrorResolver } from '@message-queue-toolkit/sns'
import { beforeAll, describe, it } from 'vitest'
import {
  errorReporter,
  logger,
  snsClient,
  sqsClient,
  stsClient,
  transactionObservabilityManager,
} from './common/Dependencies.ts'
import { publisherManager } from './common/TestPublisherManager.ts'
import { UserConsumer } from './common/UserConsumer.js'

// This test suite illustrates the importance of only initting the consumers you need
// to prevent test execution time from increasing with every new consumer added
describe('Consumer init', () => {
  beforeAll(async () => {
    await publisherManager.initRegisteredPublishers([UserConsumer.SUBSCRIBED_TOPIC_NAME])
  })

  it('Inits one consumer', async () => {
    const userConsumer = new UserConsumer({
      errorReporter,
      logger,
      transactionObservabilityManager,
      consumerErrorResolver: new SnsConsumerErrorResolver(),
      sqsClient,
      snsClient,
      stsClient,
    })

    await userConsumer.start()
    await userConsumer.close()
  })

  it('Inits twenty consumers', async () => {
    const consumers: UserConsumer[] = []
    for (let i = 0; i < 20; i++) {
      consumers.push(
        new UserConsumer({
          errorReporter,
          logger,
          transactionObservabilityManager,
          consumerErrorResolver: new SnsConsumerErrorResolver(),
          sqsClient,
          snsClient,
          stsClient,
        }),
      )
    }

    for (const userConsumer of consumers) {
      await userConsumer.start()
    }

    for (const userConsumer of consumers) {
      await userConsumer.close()
    }
  })
})
