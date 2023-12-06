import { HandlerSpy } from '../../lib/queues/HandlerSpy'

type Message = {
  id: string
  status: string
}

const TEST_MESSAGE: Message = {
  id: 'abc',
  status: 'done',
}

const TEST_MESSAGE_2: Message = {
  id: 'abc',
  status: 'inprogress',
}

describe('HandlerSpy', () => {
  describe('clear', () => {
    it('Remove stored events', () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'success',
        message: TEST_MESSAGE_2,
      })

      spy.clear()

      // @ts-ignore
      expect(spy.spyPromises).toHaveLength(0)
    })
  })

  describe('waitForEvent', () => {
    it('Finds previously consumed event', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'success',
        message: TEST_MESSAGE_2,
      })

      spy.addProcessedMessage({
        processingResult: 'success',
        message: TEST_MESSAGE,
      })

      const message = await spy.waitForEvent({
        status: 'done',
      })

      expect(message.message).toEqual(TEST_MESSAGE)
    })

    it('Waits for an event to be consumed', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'success',
        message: TEST_MESSAGE_2,
      })

      const spyPromise = spy.waitForEvent({
        status: 'done',
      })

      spy.addProcessedMessage({
        processingResult: 'success',
        message: TEST_MESSAGE,
      })

      const message = await spyPromise

      expect(message.message).toEqual(TEST_MESSAGE)
    })
  })
})
