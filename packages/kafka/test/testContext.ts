/*
import type { CommonLogger } from '@lokalise/node-core'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

// @ts-ignore
const TestLogger: CommonLogger = console

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

    redis: asFunction(
      () => {
        const redisConfig = TEST_REDIS_CONFIG

        return new Redis({
          host: redisConfig.host,
          db: redisConfig.db,
          port: redisConfig.port,
          username: redisConfig.username,
          password: redisConfig.password,
          connectTimeout: redisConfig.connectTimeout,
          commandTimeout: redisConfig.commandTimeout,
          tls: redisConfig.useTls ? {} : undefined,
          maxRetriesPerRequest: null,
          lazyConnect: true, // connect handled by asyncInit
        })
      },
      {
        asyncInitPriority: 0, // starting at the very beginning
        asyncInit: 'connect',
        dispose: (redis) => {
          return new Promise((resolve) => {
            void redis.quit((_err, result) => {
              return resolve(result)
            })
          })
        },
        lifetime: Lifetime.SINGLETON,
      },
    ),

    // vendor-specific dependencies
    transactionObservabilityManager: asFunction(() => {
      return undefined
    }, SINGLETON_CONFIG),
    messageMetricsManager: asFunction(() => undefined, SINGLETON_CONFIG),
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
  logger: CommonLogger
  sqsClient: SQSClient
  snsClient: SNSClient
  stsClient: STSClient
  s3: S3
  awilixManager: AwilixManager
  redis: Redis

  // vendor-specific dependencies
  transactionObservabilityManager: TransactionObservabilityManager
  messageMetricsManager: MessageMetricsManager

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
*/
