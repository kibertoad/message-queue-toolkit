import type { CommonLogger } from '@lokalise/node-core'
import {
  type AwilixContainer,
  Lifetime,
  type NameAndRegistrationPair,
  asFunction,
  createContainer,
} from 'awilix'
import { AwilixManager } from 'awilix-manager'

const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

type DiConfig = NameAndRegistrationPair<Dependencies>

export type TestContext = AwilixContainer<Dependencies>

export interface Dependencies {
  awilixManager: AwilixManager
  logger: CommonLogger
  kafkaConfig: KafkaConfig
}

export async function registerDependencies(): Promise<TestContext> {
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

type KafkaConfig = { brokers: string[] }

const TEST_KAFKA_CONFIG: KafkaConfig = {
  brokers: ['localhost:9092'],
}
// @ts-expect-error
const TEST_LOGGER: CommonLogger = console

const resolveDIConfig = (awilixManager: AwilixManager): DiConfig => ({
  awilixManager: asFunction(() => awilixManager, SINGLETON_CONFIG),
  logger: asFunction(() => TEST_LOGGER, SINGLETON_CONFIG),
  kafkaConfig: asFunction(() => TEST_KAFKA_CONFIG, SINGLETON_CONFIG),
})
