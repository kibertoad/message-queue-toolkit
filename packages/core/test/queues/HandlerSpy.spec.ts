import { HandlerSpy } from '../../lib/queues/HandlerSpy'

type Message = {
  id: string
  status: string
}

const TEST_MESSAGE: Message = {
  id: 'abc',
  status: 'done',
}

describe('HandlerSpy', () => {
  describe('waitForEvent', () => {
    it('Finds previously consumed event', async () => {
      const spy = new HandlerSpy<Message>()

      spy.addProcessedMessage({
        processingResult: 'success',
        message: TEST_MESSAGE,
      })

      const message = await spy.waitForEvent({
        status: 'done',
      })

      expect(message.message).toEqual(TEST_MESSAGE)
    })
  })
})
