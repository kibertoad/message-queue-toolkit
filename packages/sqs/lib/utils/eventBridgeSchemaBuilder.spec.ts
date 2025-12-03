import { describe, expect, it } from 'vitest'
import z from 'zod/v4'

import {
  createEventBridgeSchema,
  createEventBridgeSchemas,
  type EventBridgeDetail,
} from './eventBridgeSchemaBuilder.ts'

describe('eventBridgeSchemaBuilder', () => {
  describe('createEventBridgeSchema', () => {
    it('should create EventBridge schema without literal detail-type', () => {
      const detailSchema = z.object({
        userId: z.string(),
        action: z.string(),
      })

      const eventSchema = createEventBridgeSchema(detailSchema)

      const validEvent = {
        version: '0',
        id: '123',
        'detail-type': 'user.action',
        source: 'app',
        account: '123456',
        time: '2025-11-18T12:00:00Z',
        region: 'us-east-1',
        resources: [],
        detail: {
          userId: 'user123',
          action: 'login',
        },
      }

      const result = eventSchema.safeParse(validEvent)
      expect(result.success).toBe(true)
    })

    it('should create EventBridge schema with literal detail-type', () => {
      const detailSchema = z.object({
        userId: z.string(),
        action: z.string(),
      })

      const eventSchema = createEventBridgeSchema(detailSchema, 'user.login')

      const validEvent = {
        version: '0',
        id: '123',
        'detail-type': 'user.login',
        source: 'app',
        account: '123456',
        time: '2025-11-18T12:00:00Z',
        region: 'us-east-1',
        resources: [],
        detail: {
          userId: 'user123',
          action: 'login',
        },
      }

      const result = eventSchema.safeParse(validEvent)
      expect(result.success).toBe(true)
    })

    it('should fail validation with wrong detail-type literal', () => {
      const detailSchema = z.object({
        userId: z.string(),
        action: z.string(),
      })

      const eventSchema = createEventBridgeSchema(detailSchema, 'user.login')

      const invalidEvent = {
        version: '0',
        id: '123',
        'detail-type': 'user.logout', // Wrong literal
        source: 'app',
        account: '123456',
        time: '2025-11-18T12:00:00Z',
        region: 'us-east-1',
        resources: [],
        detail: {
          userId: 'user123',
          action: 'login',
        },
      }

      const result = eventSchema.safeParse(invalidEvent)
      expect(result.success).toBe(false)
    })

    it('should validate detail field against provided schema', () => {
      const detailSchema = z.object({
        userId: z.string(),
        count: z.number(),
      })

      const eventSchema = createEventBridgeSchema(detailSchema)

      const invalidEvent = {
        version: '0',
        id: '123',
        'detail-type': 'user.action',
        source: 'app',
        account: '123456',
        time: '2025-11-18T12:00:00Z',
        region: 'us-east-1',
        resources: [],
        detail: {
          userId: 'user123',
          count: 'not-a-number', // Invalid type
        },
      }

      const result = eventSchema.safeParse(invalidEvent)
      expect(result.success).toBe(false)
    })
  })

  describe('createEventBridgeSchemas', () => {
    it('should create multiple EventBridge schemas', () => {
      const schemas = createEventBridgeSchemas({
        userCreated: z.object({
          userId: z.string(),
          email: z.string(),
        }),
        userUpdated: z.object({
          userId: z.string(),
          changes: z.record(z.string(), z.unknown()),
        }),
        userDeleted: z.object({
          userId: z.string(),
        }),
      })

      expect(schemas.userCreated).toBeDefined()
      expect(schemas.userUpdated).toBeDefined()
      expect(schemas.userDeleted).toBeDefined()
    })

    it('should validate events with created schemas', () => {
      const schemas = createEventBridgeSchemas({
        userCreated: z.object({
          userId: z.string(),
          email: z.string(),
        }),
        userDeleted: z.object({
          userId: z.string(),
        }),
      })

      const validUserCreatedEvent = {
        version: '0',
        id: '123',
        'detail-type': 'user.created',
        source: 'app',
        account: '123456',
        time: '2025-11-18T12:00:00Z',
        region: 'us-east-1',
        resources: [],
        detail: {
          userId: 'user123',
          email: 'test@example.com',
        },
      }

      const validUserDeletedEvent = {
        version: '0',
        id: '456',
        'detail-type': 'user.deleted',
        source: 'app',
        account: '123456',
        time: '2025-11-18T12:00:00Z',
        region: 'us-east-1',
        resources: [],
        detail: {
          userId: 'user123',
        },
      }

      expect(schemas.userCreated.safeParse(validUserCreatedEvent).success).toBe(true)
      expect(schemas.userDeleted.safeParse(validUserDeletedEvent).success).toBe(true)
    })

    it('should handle empty schema object', () => {
      const schemas = createEventBridgeSchemas({})

      expect(Object.keys(schemas)).toHaveLength(0)
    })

    it('should preserve schema keys', () => {
      const schemas = createEventBridgeSchemas({
        foo: z.object({ bar: z.string() }),
        baz: z.object({ qux: z.number() }),
      })

      expect(Object.keys(schemas)).toEqual(['foo', 'baz'])
    })
  })

  describe('EventBridgeDetail type helper', () => {
    it('should extract detail type from EventBridge schema', () => {
      const detailSchema = z.object({
        userId: z.string(),
        action: z.string(),
      })

      const eventSchema = createEventBridgeSchema(detailSchema)

      type Detail = EventBridgeDetail<typeof eventSchema>

      const detail: Detail = {
        userId: 'user123',
        action: 'login',
      }

      expect(detail.userId).toBe('user123')
      expect(detail.action).toBe('login')
    })
  })
})
