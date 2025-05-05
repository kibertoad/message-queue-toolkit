import type { CommonLogger } from '@lokalise/node-core'
import { type AwilixContainer, Lifetime, type Resolver, asFunction, createContainer } from 'awilix'
import { AwilixManager } from 'awilix-manager'

const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

// biome-ignore lint/suspicious/noExplicitAny: Expected
type DiConfig = Record<keyof Dependencies, Resolver<any>>

export type DependencyOverrides = Partial<DiConfig>

export type TestContext = AwilixContainer<Dependencies>

export interface Dependencies {
  logger: CommonLogger
  awilixManager: AwilixManager
  kafkaConfig: KafkaConfig
}

export async function registerDependencies(
  dependencyOverrides: DependencyOverrides = {},
): Promise<TestContext> {
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
    logger: asFunction(() => console, SINGLETON_CONFIG),
    awilixManager: asFunction(() => awilixManager, SINGLETON_CONFIG),
    kafkaConfig: asFunction(() => TEST_KAFKA_CONFIG, SINGLETON_CONFIG),
  }
  diContainer.register(diConfig)

  for (const [dependencyKey, dependencyValue] of Object.entries(dependencyOverrides)) {
    diContainer.register(dependencyKey, dependencyValue)
  }

  await awilixManager.executeInit()

  return diContainer
}

type KafkaConfig = { brokers: string[] }

const TEST_KAFKA_CONFIG: KafkaConfig = {
  brokers: ['localhost:9092'],
}
