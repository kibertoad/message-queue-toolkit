import type { GetQueueAttributesCommandOutput, SQSClient } from '@aws-sdk/client-sqs'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  detectFifoQueue,
  getQueueAttributes,
  isFifoQueueName,
  validateFifoQueueConfiguration,
  validateFifoQueueName,
} from '../../lib/utils/sqsUtils.ts'

describe('sqsUtils - FIFO', () => {
  describe('isFifoQueueName', () => {
    it('returns true for queue names ending with .fifo', () => {
      expect(isFifoQueueName('my-queue.fifo')).toBe(true)
      expect(isFifoQueueName('test.fifo')).toBe(true)
      expect(isFifoQueueName('some-long-name-123.fifo')).toBe(true)
    })

    it('returns false for queue names not ending with .fifo', () => {
      expect(isFifoQueueName('my-queue')).toBe(false)
      expect(isFifoQueueName('my-queue.fifo.backup')).toBe(false)
      expect(isFifoQueueName('fifo')).toBe(false)
      expect(isFifoQueueName('')).toBe(false)
    })
  })

  describe('validateFifoQueueName', () => {
    it('does not throw for valid FIFO queue names', () => {
      expect(() => validateFifoQueueName('my-queue.fifo', true)).not.toThrow()
      expect(() => validateFifoQueueName('test.fifo', true)).not.toThrow()
    })

    it('does not throw for valid standard queue names', () => {
      expect(() => validateFifoQueueName('my-queue', false)).not.toThrow()
      expect(() => validateFifoQueueName('test', false)).not.toThrow()
    })

    it('throws when FIFO queue name does not end with .fifo', () => {
      expect(() => validateFifoQueueName('my-queue', true)).toThrow(
        /FIFO queue names must end with .fifo suffix/,
      )
      expect(() => validateFifoQueueName('test', true)).toThrow(
        /FIFO queue names must end with .fifo suffix/,
      )
    })

    it('throws when non-FIFO queue name ends with .fifo', () => {
      expect(() => validateFifoQueueName('my-queue.fifo', false)).toThrow(
        /fifoQueue option is not set to true/,
      )
      expect(() => validateFifoQueueName('test.fifo', false)).toThrow(
        /fifoQueue option is not set to true/,
      )
    })
  })

  describe('validateFifoQueueConfiguration', () => {
    it('does not throw for valid FIFO configuration with explicit flag', () => {
      expect(() =>
        validateFifoQueueConfiguration('my-queue.fifo', { FifoQueue: 'true' }, true),
      ).not.toThrow()
    })

    it('does not throw for valid FIFO configuration without explicit flag', () => {
      expect(() =>
        validateFifoQueueConfiguration('my-queue.fifo', { FifoQueue: 'true' }),
      ).not.toThrow()
    })

    it('does not throw for valid standard queue configuration', () => {
      expect(() => validateFifoQueueConfiguration('my-queue', {}, false)).not.toThrow()
      expect(() =>
        validateFifoQueueConfiguration('my-queue', { FifoQueue: 'false' }, false),
      ).not.toThrow()
    })

    it('throws when explicit FIFO flag does not match name', () => {
      expect(() => validateFifoQueueConfiguration('my-queue', {}, true)).toThrow(
        /FIFO queue names must end with .fifo suffix/,
      )
      expect(() => validateFifoQueueConfiguration('my-queue.fifo', {}, false)).toThrow(
        /fifoQueue option is not set to true/,
      )
    })

    it('throws when FIFO attribute does not match explicit flag', () => {
      expect(() =>
        validateFifoQueueConfiguration('my-queue.fifo', { FifoQueue: 'false' }, true),
      ).toThrow(/FifoQueue attribute .* does not match fifoQueue option/)

      expect(() =>
        validateFifoQueueConfiguration('my-queue', { FifoQueue: 'true' }, false),
      ).toThrow(/FifoQueue attribute .* does not match fifoQueue option/)
    })

    it('throws when queue name ends with .fifo but attribute is not set', () => {
      expect(() => validateFifoQueueConfiguration('my-queue.fifo', { FifoQueue: 'false' })).toThrow(
        /Queue name ends with .fifo but FifoQueue attribute is not set to 'true'/,
      )
      expect(() => validateFifoQueueConfiguration('my-queue.fifo', {})).toThrow(
        /Queue name ends with .fifo but FifoQueue attribute is not set to 'true'/,
      )
    })

    it('throws when FIFO attribute is true but name does not end with .fifo', () => {
      expect(() => validateFifoQueueConfiguration('my-queue', { FifoQueue: 'true' })).toThrow(
        /FIFO queue names must end with .fifo suffix/,
      )
    })

    it('does not throw when queue name and attributes are consistent for FIFO', () => {
      expect(() =>
        validateFifoQueueConfiguration('my-queue.fifo', { FifoQueue: 'true' }),
      ).not.toThrow()
    })

    it('does not throw when queue name and attributes are consistent for standard queues', () => {
      expect(() => validateFifoQueueConfiguration('my-queue', {})).not.toThrow()
      expect(() => validateFifoQueueConfiguration('my-queue', { FifoQueue: 'false' })).not.toThrow()
    })

    it('does not throw when only queue name is provided for standard queue', () => {
      expect(() => validateFifoQueueConfiguration('my-queue', undefined, false)).not.toThrow()
    })

    it('does not throw when only queue name is provided for FIFO queue with explicit flag', () => {
      expect(() => validateFifoQueueConfiguration('my-queue.fifo', undefined, true)).not.toThrow()
    })
  })

  describe('detectFifoQueue', () => {
    let mockSqsClient: SQSClient

    beforeEach(() => {
      mockSqsClient = {
        send: vi.fn(),
      } as unknown as SQSClient
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('returns true when queue name ends with .fifo', async () => {
      const result = await detectFifoQueue(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo',
        'my-queue.fifo',
      )

      expect(result).toBe(true)
      // Should not call getQueueAttributes if name check passes
      expect(mockSqsClient.send).not.toHaveBeenCalled()
    })

    it('returns true when FifoQueue attribute is true', async () => {
      ;(vi.spyOn(mockSqsClient, 'send') as any).mockResolvedValue({
        Attributes: {
          FifoQueue: 'true',
        },
      } as GetQueueAttributesCommandOutput)

      const result = await detectFifoQueue(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'my-queue',
      )

      expect(result).toBe(true)
      expect(mockSqsClient.send).toHaveBeenCalledOnce()
    })

    it('returns false when FifoQueue attribute is not true', async () => {
      ;(vi.spyOn(mockSqsClient, 'send') as any).mockResolvedValue({
        Attributes: {
          FifoQueue: 'false',
        },
      } as GetQueueAttributesCommandOutput)

      const result = await detectFifoQueue(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'my-queue',
      )

      expect(result).toBe(false)
      expect(mockSqsClient.send).toHaveBeenCalledOnce()
    })

    it('returns false when queue attributes cannot be fetched', async () => {
      ;(vi.spyOn(mockSqsClient, 'send') as any).mockResolvedValue({
        Attributes: undefined,
      } as GetQueueAttributesCommandOutput)

      const result = await detectFifoQueue(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'my-queue',
      )

      expect(result).toBe(false)
      expect(mockSqsClient.send).toHaveBeenCalledOnce()
    })

    it('returns false when getQueueAttributes returns error', async () => {
      // Mock getQueueAttributes to return error
      ;(vi.spyOn(mockSqsClient, 'send') as any).mockResolvedValue({
        Attributes: undefined,
      } as GetQueueAttributesCommandOutput)

      const result = await detectFifoQueue(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/non-existent',
        'non-existent',
      )

      expect(result).toBe(false)
    })

    it('calls getQueueAttributes without queueName parameter', async () => {
      ;(vi.spyOn(mockSqsClient, 'send') as any).mockResolvedValue({
        Attributes: {
          FifoQueue: 'true',
        },
      } as GetQueueAttributesCommandOutput)

      const result = await detectFifoQueue(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo',
      )

      expect(result).toBe(true)
      // Should call getQueueAttributes since queueName is not provided
      expect(mockSqsClient.send).toHaveBeenCalledOnce()
    })
  })

  describe('getQueueAttributes', () => {
    let mockSqsClient: SQSClient

    beforeEach(() => {
      mockSqsClient = {
        send: vi.fn(),
      } as unknown as SQSClient
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('returns queue attributes successfully', async () => {
      ;(vi.spyOn(mockSqsClient, 'send') as any).mockResolvedValue({
        Attributes: {
          FifoQueue: 'true',
          ContentBasedDeduplication: 'true',
          QueueArn: 'arn:aws:sqs:us-east-1:123456789012:my-queue.fifo',
        },
      } as GetQueueAttributesCommandOutput)

      const result = await getQueueAttributes(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo',
        ['FifoQueue', 'ContentBasedDeduplication', 'QueueArn'],
      )

      expect(result.result?.attributes).toEqual({
        FifoQueue: 'true',
        ContentBasedDeduplication: 'true',
        QueueArn: 'arn:aws:sqs:us-east-1:123456789012:my-queue.fifo',
      })
      expect(result.error).toBeUndefined()
    })

    it('uses default "All" attributes when none specified', async () => {
      ;(vi.spyOn(mockSqsClient, 'send') as any).mockResolvedValue({
        Attributes: {
          FifoQueue: 'false',
        },
      } as GetQueueAttributesCommandOutput)

      const result = await getQueueAttributes(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      )

      expect(result.result?.attributes).toEqual({
        FifoQueue: 'false',
      })
    })

    it('returns not_found error when queue does not exist', async () => {
      const error = new Error('Queue does not exist')
      // @ts-expect-error - Adding Code property to error
      error.Code = 'AWS.SimpleQueueService.NonExistentQueue'
      vi.spyOn(mockSqsClient, 'send').mockRejectedValue(error)

      const result = await getQueueAttributes(
        mockSqsClient,
        'http://sqs.us-east-1.amazonaws.com/123456789012/non-existent',
      )

      expect(result.error).toBe('not_found')
      expect(result.result).toBeUndefined()
    })

    it('throws error for other failures', async () => {
      const error = new Error('Some other error')
      vi.spyOn(mockSqsClient, 'send').mockRejectedValue(error)

      await expect(
        getQueueAttributes(
          mockSqsClient,
          'http://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        ),
      ).rejects.toThrow('Some other error')
    })
  })
})
