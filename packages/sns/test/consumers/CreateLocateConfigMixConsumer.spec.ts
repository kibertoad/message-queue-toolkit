import type { SNSClient } from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'
import type { AwilixContainer } from 'awilix'
import { beforeAll, beforeEach, describe, it } from 'vitest'
import { assertTopic } from '../../lib/utils/snsUtils.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import { type Dependencies, registerDependencies } from '../utils/testContext.ts'
import { CreateLocateConfigMixConsumer } from './CreateLocateConfigMixConsumer.ts'

describe('CreateLocateConfigMixConsumer', () => {
  let diContainer: AwilixContainer<Dependencies>
  let snsClient: SNSClient
  let stsClient: STSClient
  let testAdmin: TestAwsResourceAdmin

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    snsClient = diContainer.cradle.snsClient
    stsClient = diContainer.cradle.stsClient
    testAdmin = diContainer.cradle.testAdmin
  })

  beforeEach(async () => {
    await testAdmin.purge(CreateLocateConfigMixConsumer.CONSUMED_QUEUE_NAME)
  })

  it('accepts mixed config of create and locate', async () => {
    await assertTopic(snsClient, stsClient, {
      Name: CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME,
    })
    const consumer = new CreateLocateConfigMixConsumer(diContainer.cradle)
    await consumer.init()
  })
})
