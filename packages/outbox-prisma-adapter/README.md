# outbox-prisma-adapter

This package provides a Prisma adapter for the Outbox pattern.

### Development

#### Tests

To run the tests, you need to have a PostgreSQL database running. You can use the following command to start a PostgreSQL database using Docker:

```sh
docker-compose up -d
```

Then update Prisma client:
```sh
npx prisma generate --schema=./test/schema.prisma
```
