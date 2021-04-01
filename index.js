const { Client } = require("pg");
const path = require("path");
const pgtools = require('pgtools');
const fs = require("fs");
const { promisify } = require("util");
const { sortBy, groupBy, values, get } = require("lodash");

const readFile = promisify(fs.readFile);

async function getClient(config) {
  const client = new Client(config);
  await client.connect();
  return client;
}

async function createdb(config) {
  try {
    await promisify(pgtools.createdb)({
      ...config.pg,
      database: undefined
    }, config.pg.database);
  } catch (error) {
    if (error.name != "duplicate_database") {
      throw error;
    }
  }
}

async function dropdb(config) {
  try {
    await promisify(pgtools.dropdb)({
      ...config.pg,
      database: undefined
    }, config.pg.database);
  } catch (error) {
    throw error;
  }
}

async function getMigrations(config) {
  const migrationFiles = await promisify(fs.readdir)(config.dir);
  const migrations = migrationFiles.map((filename) => {
    const fragments = filename.split("_");
    return {
      id: parseInt(fragments[0]),
      side: fragments[1],
      filename,
    };
  });
  return sortBy(values(groupBy(migrations, "id")).map((group) => ({
    id: group[0].id,
    up: get(group.find((i) => i.side == "up") || {}, "filename"),
    down: get(group.find((i) => i.side == "down") || {}, "filename"),
  })), "id");
}

async function getMigrationsDone(client) {
  return client.query(`SELECT id, migration_id, migration_run_id FROM migrations`);
}

async function ensureMigrationsTables(client) {
  await client.query(`CREATE TABLE IF NOT EXISTS migration_runs (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);

  await client.query(`CREATE TABLE IF NOT EXISTS migrations (
    id SERIAL PRIMARY KEY,
    migration_id INT NOT NULL,
    migration_run_id INT NOT NULL,
    CONSTRAINT fk_migration_runs FOREIGN KEY (migration_run_id) REFERENCES migration_runs(id)
  )`);
}

async function recreate(config) {
  await dropdb(config);
  await migrate(config);
}

async function migrate(config) {
  await createdb(config);
  const migrations = await getMigrations(config);
  const client = await getClient(config);

  await ensureMigrationsTables(client);

  await client.query(`BEGIN`);

  try {
    const doneMigrations = await getMigrationsDone(client);
    const doneMigrationIds = doneMigrations.rows.map(({ migration_id }) => migration_id);
    const missingMigrations = migrations.filter(({ id }) => !doneMigrationIds.includes(id));

    if (missingMigrations.length > 0) {
      const insertResult = await client.query(`INSERT INTO migration_runs DEFAULT VALUES RETURNING id`);
      const migrationRunId = insertResult.rows[0].id;

      for (const { id, up } of missingMigrations) {
        console.log(`RUNNING ${up}`);
        const sql = await readFile(path.join(dir, up), "utf-8");
        await runSqlFileContent(client, sql);
        await client.query(`INSERT INTO migrations (migration_id, migration_run_id) VALUES ($1, $2)`, [ id, migrationRunId ])
      }
      await client.query(`COMMIT`);
    } else {
      console.log(`NO MIGRATIONS PENDING`);
      await client.query(`ROLLBACK`);
    }
  } catch (error) {
    await client.query(`ROLLBACK`);
    throw error;
  }

  await client.end();
}

async function rollback(config) {
  await createdb(config);
  const migrations = await getMigrations(config);
  const client = await getClient(config);

  await ensureMigrationsTables(client);

  await client.query(`BEGIN`);

  try {
    const lastRunResult = await client.query(`SELECT id FROM migration_runs ORDER BY created_at DESC LIMIT 1`);
    const lastRunId = lastRunResult.rows[0].id;
    const doneMigrations = await getMigrationsDone(client);
    const rollbackIds = doneMigrations.rows
      .filter(({ migration_run_id }) => migration_run_id == lastRunId)
      .map(({ migration_id }) => migration_id);

    const rollbackMigrations = migrations.filter(({ id }) => rollbackIds.includes(id));

    if (rollbackMigrations.length > 0) {
      for (const { down } of rollbackMigrations) {
        console.log(`RUNNING ${down}`);
        const sql = await readFile(path.join(dir, down), "utf-8");
        await runSqlFileContent(client, sql);
      }
      await client.query(`DELETE FROM migrations WHERE migration_run_id = $1`, [ lastRunId ]);
      await client.query(`DELETE FROM migration_runs WHERE id = $1`, [ lastRunId ]);
      await client.query(`COMMIT`);
    } else {
      console.log(`NO MIGRATIONS PENDING`);
      await client.query(`ROLLBACK`);
    }
  } catch (error) {
    await client.query(`ROLLBACK`);
    throw error;
  }

  await client.end();
}

async function runSqlFileContent(client, sql) {
  await client.query(sql);
}

module.exports = {
  rollback, migrate, recreate, createdb, dropdb
}
