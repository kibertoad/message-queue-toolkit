import { expect } from 'vitest'

import { HandlerSpy, isHandlerSpy } from '../../lib/queues/HandlerSpy'

type Message = {
  id: string
  status: string
  payload?: Record<string, unknown>
}

const TEST_MESSAGE: Message = {
  id: 'abc',
  status: 'done',
}

const TEST_MESSAGE_2: Message = {
  id: 'abcd',
  status: 'inprogress',
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

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_2,
      })

      spy.clear()

      // @ts-ignore
      expect(spy.spyPromises).toHaveLength(0)
    })
  })

  describe('waitForMessage', () => {
    it('Finds previously consumed event', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_2,
      })

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE,
      })

      const message = await spy.waitForMessage({
        status: 'done',
      })

      expect(message.message).toEqual(TEST_MESSAGE)
    })

    it('Finds previously consumed event with a deep match', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_3,
      })

      const message = await spy.waitForMessage({
        payload: {
          projectId: 1,
        },
      })

      expect(message.message).toEqual(TEST_MESSAGE_3)
    })

    it('Finds previously consumed event with a deep partial match', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_3,
      })

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_4,
      })

      const message = await spy.waitForMessage({
        payload: {
          projectId: 1,
          userId: 2,
        },
      })

      expect(message.message).toEqual(TEST_MESSAGE_4)
    })

    it('Finds previously consumed event with an array match in order', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_5A,
      })

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_5B,
      })

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

      spy.addProcessedMessage({
        processingResult: 'retryLater',
        message: TEST_MESSAGE,
      })

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE,
      })

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

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_2,
      })

      const spyPromise = spy.waitForMessage({
        status: 'done',
      })

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE,
      })

      const message = await spyPromise

      expect(message.message).toEqual(TEST_MESSAGE)
    })
  })

  describe('waitForMessageById', () => {
    it('Waits for an message to be consumed by id', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE_2,
      })

      const spyPromise = spy.waitForMessageWithId(TEST_MESSAGE.id)

      spy.addProcessedMessage({
        processingResult: 'consumed',
        message: TEST_MESSAGE,
      })

      const message = await spyPromise

      expect(message.message).toEqual(TEST_MESSAGE)
    })

    it('Waits for an invalid message to be rejected by id', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage(
        {
          processingResult: 'invalid_message',
          message: null,
        },
        'abc',
      )

      const messageResult = await spy.waitForMessageWithId('abc')

      expect(messageResult.message).toEqual({
        id: 'abc',
      })
      expect(messageResult.processingResult).toBe('invalid_message')
    })
  })

  describe('isHandlerSpy', () => {
    it('HandlerSpy returns true', async () => {
      const spy = new HandlerSpy<Message>()

      expect(isHandlerSpy(spy)).toBe(true)
    })

    it('Not a HandlerSpy returns false', async () => {
      expect(isHandlerSpy({})).toBe(false)
      expect(isHandlerSpy('abc')).toBe(false)
      expect(isHandlerSpy(null)).toBe(false)
      expect(isHandlerSpy(undefined)).toBe(false)
    })
  })
})
