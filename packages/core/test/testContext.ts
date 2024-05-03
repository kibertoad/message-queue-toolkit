import type { ErrorReporter } from '@lokalise/node-core'
import type { Resolver } from 'awilix'
import { asClass, asFunction, createContainer, Lifetime } from 'awilix'
import { AwilixManager } from 'awilix-manager'
import type { Logger } from 'pino'
import pino from 'pino'
import { z } from 'zod'

import { DomainEventEmitter } from '../lib/events/DomainEventEmitter'
import { EventRegistry } from '../lib/events/EventRegistry'
import type { CommonEventDefinition } from '../lib/events/eventTypes'
import { BASE_MESSAGE_SCHEMA } from '../lib/messages/baseMessageSchemas'
import type { TransactionObservabilityManager } from '../lib/types/MessageQueueTypes'

export const SINGLETON_CONFIG = { lifetime: Lifetime.SINGLETON }

export type DependencyOverrides = Partial<DiConfig>

const TestLogger: Logger = pino()

export const TestEvents = {
  created: {
    schema: BASE_MESSAGE_SCHEMA.extend({
      type: z.literal('entity.created'),
      payload: z.object({
        message: z.string(),
      }),
    }),
  },

  updated: {
    schema: BASE_MESSAGE_SCHEMA.extend({
      type: z.literal('entity.updated'),
      payload: z.object({
        message: z.string(),
      }),
    }),
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

    eventEmitter: asClass(DomainEventEmitter, SINGLETON_CONFIG),

    // vendor-specific dependencies
    newRelicBackgroundTransactionManager: asFunction(() => {
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
type DiConfig = Record<keyof Dependencies, Resolver<any>>

export interface Dependencies {
  logger: Logger
  awilixManager: AwilixManager

  // vendor-specific dependencies
  newRelicBackgroundTransactionManager: TransactionObservabilityManager

  errorReporter: ErrorReporter
  eventRegistry: EventRegistry<TestEventsType>
  eventEmitter: DomainEventEmitter<TestEventsType>
}
