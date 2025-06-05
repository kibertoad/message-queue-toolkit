import { randomUUID } from 'node:crypto'
import type { CommonLogger, ErrorReporter } from '@lokalise/node-core'
import { Admin } from '@platformatic/kafka'
import {
  type AwilixContainer,
  Lifetime,
  type NameAndRegistrationPair,
  asFunction,
  createContainer,
} from 'awilix'
import { AwilixManager } from 'awilix-manager'
import type { KafkaConfig, KafkaDependencies } from '../../lib/index.ts'
import { TEST_KAFKA_CONFIG } from './testKafkaConfig.js'

const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

type DiConfig = NameAndRegistrationPair<Dependencies>

export type TestContext = AwilixContainer<Dependencies>

type Dependencies = {
  awilixManager: AwilixManager
  kafkaConfig: KafkaConfig
  kafkaAdmin: Admin
} & KafkaDependencies

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

// @ts-expect-error
const TEST_LOGGER: CommonLogger = console

const resolveDIConfig = (awilixManager: AwilixManager): DiConfig => ({
  awilixManager: asFunction(() => awilixManager, SINGLETON_CONFIG),
  kafkaConfig: asFunction(() => TEST_KAFKA_CONFIG, SINGLETON_CONFIG),
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
  logger: asFunction(() => TEST_LOGGER, SINGLETON_CONFIG),
  errorReporter: asFunction(
    () =>
      ({
        report: () => {},
      }) satisfies ErrorReporter,
  ),
})
