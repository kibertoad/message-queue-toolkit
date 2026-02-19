import { randomUUID } from 'node:crypto'
import {
  type CommonLogger,
  type ErrorReporter,
  globalLogger,
  type TransactionObservabilityManager,
} from '@lokalise/node-core'
import type { MessageMetricsManager } from '@message-queue-toolkit/core'
import { Admin } from '@platformatic/kafka'
import {
  type AwilixContainer,
  asFunction,
  createContainer,
  Lifetime,
  type NameAndRegistrationPair,
} from 'awilix'
import { AwilixManager } from 'awilix-manager'
import type { KafkaConfig } from '../../lib/index.ts'

const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

type DiConfig = NameAndRegistrationPair<Dependencies>

export type TestContext = AwilixContainer<Dependencies>

type Dependencies = {
  awilixManager: AwilixManager
  kafkaConfig: KafkaConfig
  kafkaAdmin: Admin
  errorReporter: ErrorReporter
  logger: CommonLogger
  transactionObservabilityManager: TransactionObservabilityManager
  messageMetricsManager: MessageMetricsManager<object>
}

export const createTestContext = async (): Promise<TestContext> => {
  const diContainer = createContainer({
    injectionMode: 'PROXY',
  })
  const awilixManager = new AwilixManager({
    diContainer,
    asyncDispose: true,
    asyncInit: true,
    eagerInject: true,
  })
  diContainer.register(resolveDIConfig(awilixManager))

  await awilixManager.executeInit()

  return diContainer
}

export const getKafkaConfig = (): KafkaConfig => ({
  bootstrapBrokers: ['localhost:9092'],
  clientId: randomUUID(),
})

const resolveDIConfig = (awilixManager: AwilixManager): DiConfig => ({
  awilixManager: asFunction(() => awilixManager, SINGLETON_CONFIG),
  kafkaConfig: asFunction(getKafkaConfig, SINGLETON_CONFIG),
  kafkaAdmin: asFunction(
    ({ kafkaConfig }) =>
      new Admin({
        clientId: randomUUID(),
        bootstrapBrokers: kafkaConfig.bootstrapBrokers,
      }),
    {
      lifetime: Lifetime.SINGLETON,
      asyncDispose: 'close',
    },
  ),
  logger: asFunction(() => globalLogger, SINGLETON_CONFIG),
  errorReporter: asFunction(
    () =>
      ({
        report: () => {},
      }) satisfies ErrorReporter,
    SINGLETON_CONFIG,
  ),
  transactionObservabilityManager: asFunction(
    () =>
      ({
        start: vi.fn(),
        stop: vi.fn(),
        startWithGroup: vi.fn(),
        addCustomAttributes: vi.fn(),
      }) satisfies TransactionObservabilityManager,
    SINGLETON_CONFIG,
  ),
  messageMetricsManager: asFunction(
    () => ({
      registerProcessedMessage: () => undefined,
    }),
    SINGLETON_CONFIG,
  ),
})
