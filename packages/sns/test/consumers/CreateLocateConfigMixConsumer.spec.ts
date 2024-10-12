import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
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

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
  })

  beforeEach(async () => {
    await deleteQueue(sqsClient, CreateLocateConfigMixConsumer.CONSUMED_QUEUE_NAME)
    await deleteTopic(snsClient, CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME)
  })

  it('accepts mixed config of create and locate', async () => {
    await assertTopic(snsClient, {
      Name: CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME,
    })
    const consumer = new CreateLocateConfigMixConsumer(diContainer.cradle)
    await consumer.init()
  })
})
