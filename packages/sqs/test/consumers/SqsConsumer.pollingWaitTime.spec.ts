import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeEach, describe, expect, it, type MockInstance, vi } from 'vitest'

import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsConsumer.ts'
import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  type PERMISSIONS_ADD_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'

type ExecutionContext = { incrementAmount: number }

class MinimalSqsConsumer extends AbstractSqsConsumer<
  PERMISSIONS_ADD_MESSAGE_TYPE,
  ExecutionContext
> {
  constructor(
    dependencies: SQSConsumerDependencies,
    queueName: string,
    consumerPollingWaitTimeSeconds: number | undefined,
  ) {
    super(
      dependencies,
      {
        creationConfig: { queue: { QueueName: queueName } },
        deletionConfig: { deleteIfExists: true },
        messageTypeResolver: { messageTypePath: 'messageType' },
        handlerSpy: true,
        ...(consumerPollingWaitTimeSeconds !== undefined ? { consumerPollingWaitTimeSeconds } : {}),
        handlers: new MessageHandlerConfigBuilder<PERMISSIONS_ADD_MESSAGE_TYPE, ExecutionContext>()
          .addConfig(PERMISSIONS_ADD_MESSAGE_SCHEMA, () =>
            Promise.resolve({ result: 'success' as const }),
          )
          .build(),
      },
      { incrementAmount: 1 },
    )
  }
}

describe('AbstractSqsConsumer polling wait time wiring', () => {
  const queueName = 'pollingWaitTimeQueue'

  let diContainer: AwilixContainer<Dependencies>
  let createSpy: MockInstance<typeof Consumer.create>

  beforeEach(async () => {
    diContainer = await registerDependencies()
    await diContainer.cradle.testAdmin.deleteQueues(queueName)

    createSpy = vi.spyOn(Consumer, 'create').mockImplementation(
      () =>
        ({
          on: () => undefined,
          start: () => undefined,
          stop: () => undefined,
        }) as unknown as ReturnType<typeof Consumer.create>,
    )
  })

  afterEach(async () => {
    createSpy.mockRestore()
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  it('defaults waitTimeSeconds to 20 (long polling)', async () => {
    const consumer = new MinimalSqsConsumer(diContainer.cradle, queueName, undefined)

    await consumer.start()

    expect(createSpy).toHaveBeenCalledTimes(1)
    expect(createSpy.mock.calls[0]?.[0]).toMatchObject({ waitTimeSeconds: 20 })

    await consumer.close()
  })

  it('passes waitTimeSeconds: 0 when consumerPollingWaitTimeSeconds is 0 (short polling opt-out)', async () => {
    const consumer = new MinimalSqsConsumer(diContainer.cradle, queueName, 0)

    await consumer.start()

    expect(createSpy).toHaveBeenCalledTimes(1)
    expect(createSpy.mock.calls[0]?.[0]).toMatchObject({ waitTimeSeconds: 0 })

    await consumer.close()
  })

  it('passes the user-provided waitTimeSeconds value through verbatim', async () => {
    const consumer = new MinimalSqsConsumer(diContainer.cradle, queueName, 5)

    await consumer.start()

    expect(createSpy).toHaveBeenCalledTimes(1)
    expect(createSpy.mock.calls[0]?.[0]).toMatchObject({ waitTimeSeconds: 5 })

    await consumer.close()
  })

  it('accepts the AWS upper bound of 20 (long polling max)', async () => {
    const consumer = new MinimalSqsConsumer(diContainer.cradle, queueName, 20)

    await consumer.start()

    expect(createSpy).toHaveBeenCalledTimes(1)
    expect(createSpy.mock.calls[0]?.[0]).toMatchObject({ waitTimeSeconds: 20 })

    await consumer.close()
  })

  it.each([
    ['above the upper bound', 21],
    ['negative', -1],
    ['fractional', 1.5],
    ['NaN', Number.NaN],
  ])('throws on invalid consumerPollingWaitTimeSeconds (%s)', (_label, value) => {
    expect(() => new MinimalSqsConsumer(diContainer.cradle, queueName, value)).toThrow(RangeError)
    expect(createSpy).not.toHaveBeenCalled()
  })
})
