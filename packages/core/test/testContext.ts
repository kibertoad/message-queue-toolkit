import type { ErrorReporter } from '@lokalise/node-core'
import type { Resolver } from 'awilix'
import { Lifetime, asFunction, createContainer } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import type { Logger } from 'pino'
import pino from 'pino'
import { z } from 'zod'

import { DomainEventEmitter } from '../lib/events/DomainEventEmitter'
import { EventRegistry } from '../lib/events/EventRegistry'
import type { CommonEventDefinition } from '../lib/events/eventTypes'
import type { MetadataFiller } from '../lib/messages/MetadataFiller'
import { CommonMetadataFiller } from '../lib/messages/MetadataFiller'
import { enrichMessageSchemaWithBase } from '../lib/messages/baseMessageSchemas'
import type { TransactionObservabilityManager } from '../lib/types/MessageQueueTypes'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

const TestLogger: Logger = pino()

export const TestEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        message: z.string(),
      }),
    ),
  },

  updated: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        message: z.string(),
      }),
    ),
  },
} as const satisfies Record<string, CommonEventDefinition>

export type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]

export async function registerDependencies(dependencyOverrides: DependencyOverrides = {}) {
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

    eventRegistry: asFunction(() => {
      return new EventRegistry(Object.values(TestEvents))
    }, SINGLETON_CONFIG),

    eventEmitter: asFunction((deps: Dependencies) => {
      return new DomainEventEmitter(deps, {
        handlerSpy: true,
      })
    }, SINGLETON_CONFIG),
    metadataFiller: asFunction(() => {
      return new CommonMetadataFiller({
        serviceId: 'test',
      })
    }, SINGLETON_CONFIG),

    // vendor-specific dependencies
    newRelicBackgroundTransactionManager: asFunction(() => {
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

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
type DiConfig = Record<keyof Dependencies, Resolver<any>>

export interface Dependencies {
  logger: Logger
  awilixManager: AwilixManager

  // vendor-specific dependencies
  newRelicBackgroundTransactionManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  eventRegistry: EventRegistry<TestEventsType>
  eventEmitter: DomainEventEmitter<TestEventsType>
  metadataFiller: MetadataFiller
}
