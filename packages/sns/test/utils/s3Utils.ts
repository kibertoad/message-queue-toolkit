import type { S3 } from '@aws-sdk/client-s3'
import { NoSuchBucket, NotFound } from '@aws-sdk/client-s3'

export async function assertBucket(s3: S3, bucketName: string) {
  try {
    await s3.headBucket({ Bucket: bucketName })
  } catch (e) {
    if (e instanceof NotFound) {
      await s3.createBucket({ Bucket: bucketName })
      return
    }
  }
}

export async function emptyBucket(s3: S3, bucketName: string) {
  try {
    const objects = await s3.listObjects({ Bucket: bucketName })
    if (objects.Contents?.length) {
      await s3.deleteObjects({
        Bucket: bucketName,
        Delete: { Objects: objects.Contents?.map((object) => ({ Key: object.Key })) },
      })
    }
  } catch (e) {
    if (e instanceof NoSuchBucket) {
      return
    }
  }
}

export async function getObjectContent(s3: S3, bucket: string, key: string) {
  const result = await s3.getObject({ Bucket: bucket, Key: key })
  return result.Body?.transformToString()
}
