import { describe, expect, it } from 'vitest'

import { HandlerSpy, isHandlerSpy, TYPE_NOT_RESOLVED } from '../../lib/queues/HandlerSpy.ts'

type Message = {
  id: string
  status: string
  payload?: Record<string, unknown>
}

type MessageB = {
  id2: string
  status2: string
  payload?: Record<string, unknown>
}

type DeepMessage = {
  id: string
  status: string
  payload?: {
    fieldA: number
    fieldB: string[]
    fieldC: string
    fieldD: boolean
  }
}

const TEST_MESSAGE: Message = {
  id: 'abc',
  status: 'done',
}

const TEST_MESSAGEB: MessageB = {
  id2: 'abc',
  status2: 'done',
}

const TEST_MESSAGE_2: Message = {
  id: 'abcd',
  status: 'inprogress',
}

const TEST_MESSAGE_2B: MessageB = {
  id2: 'abcd',
  status2: 'inprogress',
}

const TEST_MESSAGE_3: Message = {
  id: 'abcd',
  status: 'inprogress',
  payload: {
    projectId: 1,
  },
}

const TEST_MESSAGE_4: Message = {
  id: 'abcd',
  status: 'inprogress',
  payload: {
    projectId: 1,
    userId: 2,
  },
}

const TEST_MESSAGE_5A: Message = {
  id: 'abcd',
  status: 'inprogress',
  payload: {
    projectId: 1,
    userId: 2,
    tasks: ['b', 'a'],
  },
}

const TEST_MESSAGE_5B: Message = {
  id: 'abcd',
  status: 'inprogress',
  payload: {
    projectId: 1,
    userId: 2,
    tasks: ['a', 'b'],
  },
}

describe('HandlerSpy', () => {
  describe('clear', () => {
    it('Remove stored events', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_2,
        },
        undefined,
        'test.type',
      )

      spy.clear()

      // @ts-expect-error
      expect(spy.spyPromises).toHaveLength(0)
    })
  })

  describe('waitForMessage', () => {
    it('Finds previously consumed event', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_2,
        },
        undefined,
        'test.type',
      )

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE,
        },
        undefined,
        'test.type',
      )

      const message = await spy.waitForMessage({
        status: 'done',
      })

      expect(message.message).toEqual(TEST_MESSAGE)
    })

    it('Finds previously consumed event with type narrowing', async () => {
      const spy = new HandlerSpy<Message | MessageB>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_2B,
        },
        undefined,
        'test.type',
      )

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGEB,
        },
        undefined,
        'test.type',
      )

      const message = await spy.waitForMessage<MessageB>({
        status2: 'done',
      })

      expect(message.message).toEqual(TEST_MESSAGEB)
    })

    it('Finds previously consumed event with a deep match', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_3,
        },
        undefined,
        'test.type',
      )

      const message = await spy.waitForMessage({
        payload: {
          projectId: 1,
        },
      })

      expect(message.message).toEqual(TEST_MESSAGE_3)
    })

    it('Finds previously consumed event with a deep partial match', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_3,
        },
        undefined,
        'test.type',
      )

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_4,
        },
        undefined,
        'test.type',
      )

      const message = await spy.waitForMessage({
        payload: {
          projectId: 1,
          userId: 2,
        },
      })

      expect(message.message).toEqual(TEST_MESSAGE_4)
    })

    it('Finds previously consumed event with a deep partial match, with incomplete fields', async () => {
      const spy = new HandlerSpy<DeepMessage>()
      const message: DeepMessage = {
        id: 'abc',
        status: 'good',
        payload: {
          fieldA: 1,
          fieldB: ['w'],
          fieldC: 're',
          fieldD: true,
        },
      }

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message,
        },
        undefined,
        'test.type',
      )

      const messageResult = await spy.waitForMessage({
        payload: {
          fieldA: 1,
          fieldB: ['w'],
          fieldC: 're',
        },
      })

      expect(messageResult.message).toEqual(message)
    })

    it('Finds previously consumed event with an array match in order', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_5A,
        },
        undefined,
        'test.type',
      )

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_5B,
        },
        undefined,
        'test.type',
      )

      const message = await spy.waitForMessage({
        payload: {
          projectId: 1,
          userId: 2,
          tasks: ['a', 'b'],
        },
      })

      expect(message.message).toEqual(TEST_MESSAGE_5B)
    })

    it('Finds multiple previously consumed events', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'retryLater' },
          message: TEST_MESSAGE,
        },
        undefined,
        'test.type',
      )

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE,
        },
        undefined,
        'test.type',
      )

      const message = await spy.waitForMessage(
        {
          status: 'done',
        },
        'consumed',
      )

      const message2 = await spy.waitForMessage(
        {
          status: 'done',
        },
        'retryLater',
      )

      expect(message.message).toEqual(TEST_MESSAGE)
      expect(message2.message).toEqual(TEST_MESSAGE)
    })

    it('Waits for an message to be consumed', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_2,
        },
        undefined,
        'test.type',
      )

      const spyPromise = spy.waitForMessage({
        status: 'done',
      })

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE,
        },
        undefined,
        'test.type',
      )

      const message = await spyPromise

      expect(message.message).toEqual(TEST_MESSAGE)
    })
  })

  describe('waitForMessageById', () => {
    it('Waits for an message to be consumed by id', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_2,
        },
        undefined,
        'test.type',
      )

      const spyPromise = spy.waitForMessageWithId(TEST_MESSAGE.id)

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE,
        },
        undefined,
        'test.type',
      )

      const message = await spyPromise

      expect(message.message).toEqual(TEST_MESSAGE)
    })

    it('Finds previously consumed event with type narrowing', async () => {
      const spy = new HandlerSpy<Message | MessageB>({
        messageIdField: 'id2',
      })

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGE_2B,
        },
        undefined,
        'test.type',
      )

      spy.addProcessedMessage(
        {
          processingResult: { status: 'consumed' },
          message: TEST_MESSAGEB,
        },
        undefined,
        'test.type',
      )

      const message = await spy.waitForMessageWithId<MessageB>(TEST_MESSAGEB.id2)

      expect(message.message).toEqual(TEST_MESSAGEB)
    })

    it('Waits for an invalid message to be rejected by id', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: { status: 'error', errorReason: 'invalidMessage' },
          message: null,
        },
        'abc',
        TYPE_NOT_RESOLVED,
      )

      const messageResult = await spy.waitForMessageWithId('abc')

      expect(messageResult.message).toEqual({
        id: 'abc',
        type: 'FAILED_TO_RESOLVE',
      })
      expect(messageResult.processingResult).toEqual({
        status: 'error',
        errorReason: 'invalidMessage',
      })
    })
  })

  describe('counts', () => {
    it('Starts with all zeroes', () => {
      const spy = new HandlerSpy<Message>()

      expect(spy.counts).toEqual({
        consumed: 0,
        published: 0,
        retryLater: 0,
        error: 0,
      })
    })

    it('Increments consumed counter', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        { processingResult: { status: 'consumed' }, message: TEST_MESSAGE },
        undefined,
        'test.type',
      )

      expect(spy.counts.consumed).toBe(1)
      expect(spy.counts.error).toBe(0)
    })

    it('Increments error counter', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        { processingResult: { status: 'error', errorReason: 'invalidMessage' }, message: null },
        'abc',
        TYPE_NOT_RESOLVED,
      )

      expect(spy.counts.error).toBe(1)
      expect(spy.counts.consumed).toBe(0)
    })

    it('Increments retryLater counter', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        { processingResult: { status: 'retryLater' }, message: TEST_MESSAGE },
        undefined,
        'test.type',
      )

      expect(spy.counts.retryLater).toBe(1)
    })

    it('Increments published counter', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        { processingResult: { status: 'published' }, message: TEST_MESSAGE },
        undefined,
        'test.type',
      )

      expect(spy.counts.published).toBe(1)
    })

    it('Tracks multiple statuses correctly', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        { processingResult: { status: 'consumed' }, message: TEST_MESSAGE },
        undefined,
        'test.type',
      )
      spy.addProcessedMessage(
        { processingResult: { status: 'consumed' }, message: TEST_MESSAGE_2 },
        undefined,
        'test.type',
      )
      spy.addProcessedMessage(
        { processingResult: { status: 'error', errorReason: 'handlerError' }, message: null },
        'err1',
        TYPE_NOT_RESOLVED,
      )
      spy.addProcessedMessage(
        { processingResult: { status: 'retryLater' }, message: TEST_MESSAGE },
        undefined,
        'test.type',
      )

      expect(spy.counts).toEqual({
        consumed: 2,
        published: 0,
        retryLater: 1,
        error: 1,
      })
    })

    it('Resets counts on clear', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        { processingResult: { status: 'consumed' }, message: TEST_MESSAGE },
        undefined,
        'test.type',
      )
      spy.addProcessedMessage(
        { processingResult: { status: 'error', errorReason: 'invalidMessage' }, message: null },
        'abc',
        TYPE_NOT_RESOLVED,
      )

      spy.clear()

      expect(spy.counts).toEqual({
        consumed: 0,
        published: 0,
        retryLater: 0,
        error: 0,
      })
    })

    it('Returns a copy, not a reference', () => {
      const spy = new HandlerSpy<Message>()

      const countsBefore = spy.counts
      spy.addProcessedMessage(
        { processingResult: { status: 'consumed' }, message: TEST_MESSAGE },
        undefined,
        'test.type',
      )

      expect(countsBefore.consumed).toBe(0)
      expect(spy.counts.consumed).toBe(1)
    })
  })

  describe('isHandlerSpy', () => {
    it('HandlerSpy returns true', () => {
      const spy = new HandlerSpy<Message>()

      expect(isHandlerSpy(spy)).toBe(true)
    })

    it('Not a HandlerSpy returns false', () => {
      expect(isHandlerSpy({})).toBe(false)
      expect(isHandlerSpy('abc')).toBe(false)
      expect(isHandlerSpy(null)).toBe(false)
      expect(isHandlerSpy(undefined)).toBe(false)
    })
  })
})
