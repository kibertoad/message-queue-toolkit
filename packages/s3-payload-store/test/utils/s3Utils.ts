import type { S3 } from '@aws-sdk/client-s3'
import { NoSuchBucket } from '@aws-sdk/client-s3'

export async function assertBucket(s3: S3, bucketName: string) {
  try {
    await s3.headBucket({ Bucket: bucketName })
  } catch (e) {
    if (e instanceof NoSuchBucket) {
      await s3.createBucket({ Bucket: bucketName })
      return
    }
  }
}

export async function deleteBucketWithObjects(s3: S3, bucketName: string) {
  try {
    const objects = await s3.listObjects({ Bucket: 'foobar' })
    if (objects.Contents?.length) {
      await s3.deleteObjects({
        Bucket: bucketName,
        Delete: { Objects: objects.Contents?.map((object) => ({ Key: object.Key })) },
      })
    }
    await s3.deleteBucket({ Bucket: bucketName })
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

export async function objectExists(s3: S3, bucket: string, key: string) {
  try {
    await s3.headObject({ Bucket: bucket, Key: key })
    return true
  } catch (e) {
    return false
  }
}
