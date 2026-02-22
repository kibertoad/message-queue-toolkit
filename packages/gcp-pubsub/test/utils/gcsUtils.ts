import type { Storage } from '@google-cloud/storage'

export async function assertBucket(storage: Storage, bucketName: string) {
  const bucket = storage.bucket(bucketName)
  const [exists] = await bucket.exists()

  if (!exists) {
    await bucket.create()
  }

  return bucket
}

export async function emptyBuckets(storage: Storage, ...bucketNames: string[]) {
  for (const bucketName of bucketNames) {
    const bucket = storage.bucket(bucketName)
    const [exists] = await bucket.exists()

    if (!exists) {
      continue
    }

    const [files] = await bucket.getFiles()
    await Promise.all(files.map((file) => file.delete({ ignoreNotFound: true })))
  }
}
