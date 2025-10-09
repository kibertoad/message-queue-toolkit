import type { Bucket, Storage } from '@google-cloud/storage'

export async function assertEmptyBucket(storage: Storage, bucketName: string): Promise<void> {
  const bucket = storage.bucket(bucketName)

  // Create bucket if it doesn't exist
  const [exists] = await bucket.exists()
  if (!exists) {
    await bucket.create()
    return
  }

  // Delete all files in the bucket
  const [files] = await bucket.getFiles()
  await Promise.all(files.map((file) => file.delete({ ignoreNotFound: true })))
}

export async function getObjectContent(
  storage: Storage,
  bucketName: string,
  key: string,
): Promise<string> {
  const bucket = storage.bucket(bucketName)
  const file = bucket.file(key)
  const [content] = await file.download()
  return content.toString()
}

export async function objectExists(
  storage: Storage,
  bucketName: string,
  key: string,
): Promise<boolean> {
  const bucket = storage.bucket(bucketName)
  const file = bucket.file(key)
  const [exists] = await file.exists()
  return exists
}

export async function ensureBucket(storage: Storage, bucketName: string): Promise<Bucket> {
  const bucket = storage.bucket(bucketName)
  const [exists] = await bucket.exists()

  if (!exists) {
    await bucket.create()
  }

  return bucket
}
