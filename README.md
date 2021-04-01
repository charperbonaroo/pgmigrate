# Postgres Migrate

Screw DSLs. Write migrations using plain SQL.

## INSTALL

`npm i cbpgm`

## Example

```
migrations/
  1_up_my_first_migration.sql
  1_down_my_first_migration.sql
  2_up_add_users.sql
  2_down_add_users.sql
  3_up_add_posts.sql
  3_down_add_posts.sql
```

```sh
cbpgm migrate
```

## CONFIG

`cbpgm`'s cli will look for a `.cbpgm.js` file in the current working directory. If it cannot find any, it will load a default config file.

The default config will look for a `migrations` dir in the current working directory, and it will let `pg.Client` connect using the usual postgres-specific environment variables, which are documented [here](https://node-postgres.com/api/client).

```js
// .cbpgm.js
{
  pg: {
    // Options used to initialise node-progress client
    // See: https://node-postgres.com/api/client
  },
  // directory where all migrations are stored
  dir: path.join(process.cwd(), "migrations")
}
```

## Commands

- `rollback` - rollback the last run migrations
- `migrate` - create db if not exists & run all pending migrations
- `recreate` - equal to `dropdb`, `migrate`
- `createdb` - create without migrations
- `dropdb` - drop the db

## PROGRAMMATIC API

All any of the commands as a function with `config` as first param. Returns a promise. Eg `await require("cbpgm").migrate({ dir: "migrations", pg: { host: "localhost" }})`
