import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type {
  Logger,
  TransactionObservabilityManager
} from '@message-queue-toolkit/core';
import {
  BASE_MESSAGE_SCHEMA,
  CommonMetadataFiller,
  EventRegistry
} from '@message-queue-toolkit/core'
import type { Resolver } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import {z} from "zod";

import { AmqpConnectionManager } from '../../lib/AmqpConnectionManager'
import {AmqpAwareEventDefinition, AmqpQueuePublisherManager} from "../../lib/AmqpQueuePublisherManager";
import type {CommonAmqpQueuePublisher} from "../../lib/CommonAmqpPublisherFactory";
import { CommonAmqpQueuePublisherFactory} from "../../lib/CommonAmqpPublisherFactory";
import type { AmqpConfig } from '../../lib/amqpConnectionResolver'
import { AmqpConsumerErrorResolver } from '../../lib/errors/AmqpConsumerErrorResolver'
import { AmqpPermissionConsumer } from '../consumers/AmqpPermissionConsumer'
import { AmqpPermissionPublisher } from '../publishers/AmqpPermissionPublisher'


export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

export const TestEvents = {
  created: {
    schema: BASE_MESSAGE_SCHEMA.extend({
      type: z.literal('entity.created'),
      payload: z.object({
        newData: z.string(),
      }),
    }),
    schemaVersion: '1.0.1',
    queueName: 'dummy',
  },

  updated: {
    schema: BASE_MESSAGE_SCHEMA.extend({
      type: z.literal('entity.updated'),
      payload: z.object({
        updatedData: z.string(),
      }),
    }),
    queueName: 'dummy',
  },
} as const satisfies Record<string, AmqpAwareEventDefinition>

export type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]
export type TestEventPayloadsType = z.infer<TestEventsType[number]['schema']>


// @ts-ignore
const TestLogger: Logger = console

export async function registerDependencies(
  config: AmqpConfig,
  dependencyOverrides: DependencyOverrides = {},
  queuesEnabled = true,
) {
  const diContainer = createContainer({
    injectionMode: 'PROXY',
  })
  const awilixManager = new AwilixManager({
    diContainer,
    asyncDispose: true,
    asyncInit: true,
    eagerInject: true,
  })

  const diConfig: DiConfig = {
    logger: asFunction(() => {
      return TestLogger
    }, SINGLETON_CONFIG),
    awilixManager: asFunction(() => {
      return awilixManager
    }, SINGLETON_CONFIG),
    amqpConnectionManager: asFunction(
      ({ logger }: Dependencies) => {
        return new AmqpConnectionManager(config, logger)
      },
      {
        lifetime: Lifetime.SINGLETON,
        asyncInit: 'init',
        asyncDispose: 'close',
        asyncDisposePriority: 1,
      },
    ),
    consumerErrorResolver: asFunction(() => {
      return new AmqpConsumerErrorResolver()
    }),

    permissionConsumer: asClass(AmqpPermissionConsumer, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncDisposePriority: 10,
      enabled: queuesEnabled,
    }),
    permissionPublisher: asClass(AmqpPermissionPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncDisposePriority: 20,
      enabled: queuesEnabled,
    }),

    eventRegistry: asFunction(() => {
      return new EventRegistry(Object.values(TestEvents))
    }, SINGLETON_CONFIG),
    queuePublisherManager: asFunction(
        (dependencies) => {
          return new AmqpQueuePublisherManager(dependencies, {
            metadataFiller: new CommonMetadataFiller({
              serviceId: 'service',
              schemaVersion: '1.0.0',
            }),
            publisherFactory: new CommonAmqpQueuePublisherFactory(),
            newPublisherOptions: {
              handlerSpy: true,
              messageIdField: 'id',
              messageTypeField: 'type',
            },
          })
        },
        {
          lifetime: Lifetime.SINGLETON,
          enabled: queuesEnabled,
        },
    ),

    // vendor-specific dependencies
    transactionObservabilityManager: asFunction(() => {
      return undefined
    }, SINGLETON_CONFIG),
    errorReporter: asFunction(() => {
      return {
        report: () => {},
      } satisfies ErrorReporter
    }, SINGLETON_CONFIG),
  }
  diContainer.register(diConfig)

  for (const [dependencyKey, dependencyValue] of Object.entries(dependencyOverrides)) {
    diContainer.register(dependencyKey, dependencyValue)
  }

  await awilixManager.executeInit()

  return diContainer
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type DiConfig = Record<keyof Dependencies, Resolver<any>>

export interface Dependencies {
  logger: Logger
  amqpConnectionManager: AmqpConnectionManager
  awilixManager: AwilixManager

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: AmqpPermissionConsumer
  permissionPublisher: AmqpPermissionPublisher

  eventRegistry: EventRegistry<TestEventsType>
  queuePublisherManager: AmqpQueuePublisherManager<CommonAmqpQueuePublisher<TestEventPayloadsType>, TestEventsType>
}
