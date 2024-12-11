import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import { deleteQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { beforeAll, beforeEach, describe, it } from 'vitest'
import { assertTopic, deleteTopic } from '../../lib/utils/snsUtils'
import { type Dependencies, registerDependencies } from '../utils/testContext'
import { CreateLocateConfigMixConsumer } from './CreateLocateConfigMixConsumer'

describe('CreateLocateConfigMixConsumer', () => {
  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let snsClient: SNSClient
  let stsClient: STSClient

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
    stsClient = diContainer.cradle.stsClient
  })

  beforeEach(async () => {
    await deleteQueue(sqsClient, CreateLocateConfigMixConsumer.CONSUMED_QUEUE_NAME)
    await deleteTopic(snsClient, stsClient, CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME)
  })

  it('accepts mixed config of create and locate', async () => {
    await assertTopic(snsClient, stsClient, {
      Name: CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME,
    })
    const consumer = new CreateLocateConfigMixConsumer(diContainer.cradle)
    await consumer.init()
  })
})
