import { S3 } from '@aws-sdk/client-s3'
import { SNSClient } from '@aws-sdk/client-sns'
import { SQSClient } from '@aws-sdk/client-sqs'
import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { Logger, TransactionObservabilityManager } from '@message-queue-toolkit/core'
import {
  CommonMetadataFiller,
  EventRegistry,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/core'
import { FakeConsumerErrorResolver } from '@message-queue-toolkit/sqs'
import type { Resolver } from 'awilix'
import { Lifetime, asClass, asFunction, createContainer } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import { z } from 'zod'

import type { CommonSnsPublisher } from '../../lib/sns/CommonSnsPublisherFactory'
import { CommonSnsPublisherFactory } from '../../lib/sns/CommonSnsPublisherFactory'
import type { SnsAwareEventDefinition } from '../../lib/sns/SnsPublisherManager'
import { SnsPublisherManager } from '../../lib/sns/SnsPublisherManager'
import { SnsSqsPermissionConsumer } from '../consumers/SnsSqsPermissionConsumer'
import { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher'

import { STSClient } from '@aws-sdk/client-sts'
import { CreateLocateConfigMixPublisher } from '../publishers/CreateLocateConfigMixPublisher'
import { TEST_AWS_CONFIG } from './testSnsConfig'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-ignore
const TestLogger: Logger = console

export const TestEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        newData: z.string(),
      }),
    ),
    schemaVersion: '1.0.1',
    snsTopic: 'dummy',
  },

  updated: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        updatedData: z.string(),
      }),
    ),
    snsTopic: 'dummy',
  },
} as const satisfies Record<string, SnsAwareEventDefinition>

export type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]
export type TestEventPublishPayloadsType = z.infer<TestEventsType[number]['publisherSchema']>
export type TestEventConsumerPayloadsType = z.infer<TestEventsType[number]['consumerSchema']>

export async function registerDependencies(
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

    // Not disposing sqs client allows consumers to terminate correctly
    sqsClient: asFunction(
      () => {
        return new SQSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),
    snsClient: asFunction(
      () => {
        return new SNSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),
    stsClient: asFunction(
      () => {
        return new STSClient(TEST_AWS_CONFIG)
      },
      {
        lifetime: Lifetime.SINGLETON,
      },
    ),
    s3: asFunction(() => {
      return new S3(TEST_AWS_CONFIG)
    }),

    consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),

    permissionConsumer: asClass(SnsSqsPermissionConsumer, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'start',
      asyncDispose: 'close',
      asyncInitPriority: 30,
      asyncDisposePriority: 30,
      enabled: queuesEnabled,
    }),
    permissionPublisher: asClass(SnsPermissionPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncInitPriority: 40,
      asyncDisposePriority: 40,
      enabled: queuesEnabled,
    }),
    createLocateConfigMixPublisher: asClass(CreateLocateConfigMixPublisher, {
      lifetime: Lifetime.SINGLETON,
      asyncInit: 'init',
      asyncDispose: 'close',
      asyncInitPriority: 40,
      asyncDisposePriority: 40,
      enabled: queuesEnabled,
    }),
    eventRegistry: asFunction(() => {
      return new EventRegistry(Object.values(TestEvents))
    }, SINGLETON_CONFIG),
    publisherManager: asFunction(
      (dependencies) => {
        return new SnsPublisherManager(dependencies, {
          metadataFiller: new CommonMetadataFiller({
            serviceId: 'service',
          }),
          publisherFactory: new CommonSnsPublisherFactory(),
          newPublisherOptions: {
            handlerSpy: true,
            messageIdField: 'id',
            messageTypeField: 'type',
            creationConfig: {
              updateAttributesIfExists: true,
            },
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
    }),
  }
  diContainer.register(diConfig)

  for (const [dependencyKey, dependencyValue] of Object.entries(dependencyOverrides)) {
    diContainer.register(dependencyKey, dependencyValue)
  }

  await awilixManager.executeInit()

  return diContainer
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
type DiConfig = Record<keyof Dependencies, Resolver<any>>

export interface Dependencies {
  logger: Logger
  sqsClient: SQSClient
  snsClient: SNSClient
  stsClient: STSClient
  s3: S3
  awilixManager: AwilixManager

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  consumerErrorResolver: ErrorResolver
  permissionConsumer: SnsSqsPermissionConsumer
  permissionPublisher: SnsPermissionPublisher
  eventRegistry: EventRegistry<TestEventsType>
  publisherManager: SnsPublisherManager<
    CommonSnsPublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >
  createLocateConfigMixPublisher: CreateLocateConfigMixPublisher
}
