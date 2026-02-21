import type { AwilixContainer } from 'awilix'
import { beforeAll, beforeEach, describe, it } from 'vitest'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import { type Dependencies, registerDependencies } from '../utils/testContext.ts'
import { CreateLocateConfigMixConsumer } from './CreateLocateConfigMixConsumer.ts'

describe('CreateLocateConfigMixConsumer', () => {
  let diContainer: AwilixContainer<Dependencies>
  let testAdmin: TestAwsResourceAdmin

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    testAdmin = diContainer.cradle.testAdmin
  })

  beforeEach(async () => {
    await testAdmin.deleteQueues(CreateLocateConfigMixConsumer.CONSUMED_QUEUE_NAME)
    await testAdmin.deleteTopics(CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME)
  })

  it('accepts mixed config of create and locate', async () => {
    await testAdmin.createTopic(CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME)
    const consumer = new CreateLocateConfigMixConsumer(diContainer.cradle)
    await consumer.init()
  })
})
