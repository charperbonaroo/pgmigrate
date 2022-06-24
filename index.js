const { Client } = require("pg");
const path = require("path");
const pgtools = require('pgtools');
const fs = require("fs");
const { promisify } = require("util");
const ConnectionParameters = require("pg/lib/connection-parameters");

const readFile = promisify(fs.readFile);
const writeFile = promisify(fs.writeFile);

const CONFIG_FILENAME = process.env.CONFIG_FILENAME || `.cbpgm.js`;

async function getClient(config) {
  const client = new Client(config.pg);
  await client.connect();
  return client;
}

async function createdb(config) {
  try {
    const pgConfig = new ConnectionParameters(config.pg || {});
    console.log(`CREATE DATABASE IF NOT EXIST ${pgConfig.database} AS ${pgConfig.user} (password: ${pgConfig.password ? 'YES' : 'NO'})`)
    await promisify(pgtools.createdb)({
      ...pgConfig,
      database: undefined
    }, pgConfig.database);
  } catch (error) {
    if (error.name != "duplicate_database") {
      throw error;
    }
  }
}

async function dropdb(config) {
  try {
    const pgConfig = new ConnectionParameters(config.pg || {});
    console.log(`DROP DATABASE IF EXISTS ${pgConfig.database} AS ${pgConfig.user} (password: ${pgConfig.password ? 'YES' : 'NO'})`)
    await promisify(pgtools.dropdb)({
      ...pgConfig,
      database: undefined
    }, pgConfig.database);
  } catch (error) {
    throw error;
  }
}

async function getMigrations(config) {
  const realPath = fs.realpathSync(config.dir);
  const migrationPaths = fs.readdirSync(realPath, { withFileTypes: true });
  const migrationMap = migrationPaths.map((value) => {
    if (value.isFile() && value.name.endsWith(".sql")) {
      return { id: value.name.replace(/.sql$/, ""), up: path.join(realPath, value.name), down: null }
    } else if (value.isDirectory()) {
      return {
        id: value.name,
        up: exists(path.join(realPath, value.name, `up.sql`)),
        down: exists(path.join(realPath, value.name, `down.sql`))
      }
    } else {
      return null;
    }
  }).filter(_ => _ && (_.up || _.down)).reduce((acc, value) => ({ ...acc, [value.id]: value }), {});
  const keys = Object.keys(migrationMap)
  naturalSort(keys);
  return keys.map((key) => migrationMap[key]);
}

function isIgnored(config, key) {
  return config.ignore && config.ignore.includes(key);
}

function exists(path) {
  return fs.existsSync(path) ? path : null;
}

async function getMigrationsDone(client) {
  return client.query(`SELECT id, migration_id, migration_run_id FROM public.cbpgm_migrations`);
}

async function ensureMigrationsTables(client) {
  console.log(`CREATE TABLE IF NOT EXISTS public.cbpgm_migration_runs`)
  await client.query(`CREATE TABLE IF NOT EXISTS public.cbpgm_migration_runs (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);

  console.log(`CREATE TABLE IF NOT EXISTS public.cbpgm_migrations`)
  await client.query(`CREATE TABLE IF NOT EXISTS public.cbpgm_migrations (
    id SERIAL PRIMARY KEY,
    migration_id TEXT NOT NULL,
    migration_run_id INT NOT NULL,
    CONSTRAINT fk_migration_runs FOREIGN KEY (migration_run_id) REFERENCES public.cbpgm_migration_runs(id)
  )`);
}

async function recreate(config) {
  try {
    await dropdb(config);
  } catch (error) {
    if (error.message !== "Attempted to drop a database that does not exist") {
      throw error;
    }
  }
  await migrate(config);
}

async function migrate(config) {
  if (config.createOnMigrate !== false) {
    await createdb(config);
  }
  const migrations = (await getMigrations(config)).filter(({ id }) => !isIgnored(config, id));
  const client = await getClient(config);

  await ensureMigrationsTables(client);
  console.log(`START MIGRATE`);

  await client.query(`BEGIN`);

  try {
    const doneMigrations = await getMigrationsDone(client);
    const doneMigrationIds = doneMigrations.rows.map(({ migration_id }) => migration_id);
    const missingMigrations = migrations.filter(({ id }) => !doneMigrationIds.includes(id));

    if (missingMigrations.length > 0) {
      const insertResult = await client.query(`INSERT INTO public.cbpgm_migration_runs DEFAULT VALUES RETURNING id`);
      const migrationRunId = insertResult.rows[0].id;
      const realPath = fs.realpathSync(config.dir);

      for (const { id, up } of missingMigrations) {
        if (up) {
          console.log(`RUNNING ${up.replace(realPath, "").substr(1)}`);
          const sql = await readFile(up, "utf-8");
          await runSqlFileContent(client, sql);
        } else {
          console.log(`LOGGING ${id} (nothing to execute)`)
        }
        await client.query(`INSERT INTO public.cbpgm_migrations (migration_id, migration_run_id) VALUES ($1, $2)`, [id, migrationRunId])
      }
      await client.query(`COMMIT`);
      console.log(`${missingMigrations.length} MIGRATIONS DONE`);
    } else {
      console.log(`NO MIGRATIONS DONE`);
      await client.query(`ROLLBACK`);
    }
  } catch (error) {
    await client.query(`ROLLBACK`);

    throw new Error(renderError(error));
  }

  await client.end();
}

async function rollback(config) {
  await createdb(config);
  console.log(`STARTING ROLLBACK`);
  const migrations = (await getMigrations(config)).filter(({ id }) => !isIgnored(config, id)).reverse();
  const client = await getClient(config);

  await ensureMigrationsTables(client);

  await client.query(`BEGIN`);

  try {
    const lastRunResult = await client.query(`SELECT id FROM public.cbpgm_migration_runs ORDER BY created_at DESC LIMIT 1`);
    const lastRunId = lastRunResult.rows.length > 0 ? lastRunResult.rows[0].id : null;
    const doneMigrations = await getMigrationsDone(client);
    const rollbackIds = doneMigrations.rows
      .filter(({ migration_run_id }) => migration_run_id == lastRunId)
      .map(({ migration_id }) => migration_id);

    const rollbackMigrations = migrations.filter(({ id }) => rollbackIds.includes(id));
    const realPath = fs.realpathSync(config.dir);

    if (rollbackMigrations.length > 0) {
      for (const { id, down } of rollbackMigrations) {
        if (down) {
          console.log(`ROLLBACK ${down.replace(realPath, "").substr(1)}`);
          const sql = await readFile(down, "utf-8");
          await runSqlFileContent(client, sql);
        } else {
          console.log(`SKIP ${id} (no down.sql)`)
        }
      }
      await client.query(`DELETE FROM public.cbpgm_migrations WHERE migration_run_id = $1`, [lastRunId]);
      await client.query(`DELETE FROM public.cbpgm_migration_runs WHERE id = $1`, [lastRunId]);
      await client.query(`COMMIT`);
      console.log(`${rollbackMigrations.length} MIGRATIONS ROLLED BACK`);
    } else {
      console.log(`NO MIGRATIONS TO ROLLBACK`);
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

async function config(config) {
  console.log(new ConnectionParameters(config.pg));
}

async function help() {
  console.log(await readFile(path.join(__dirname, "README"), "utf-8"))
}

async function list(config) {
  const migrations = await getMigrations(config);
  let done, client;
  try {
    client = await getClient(config);
    done = await getMigrationsDone(client);
  } catch (error) {
    console.error(error);
  } finally {
    await client.end();
  }

  done = (done && done.rows || []).map((_) => _.migration_id)

  const completed = [];
  const pending = [];
  const ignored = [];

  for (const { id } of migrations) {
    if (isIgnored(config, id)) {
      ignored.push(id);
    } else if (done.includes(id)) {
      completed.push(id);
    } else {
      pending.push(id);
    }
  }

  console.log("DONE");
  if (completed.length) {
    console.log("\n  " + completed.join("\n  ") + "\n")
  } else {
    console.log(`\n  -\n`)
  }

  console.log("NEW");
  if (pending.length) {
    console.log("\n  " + pending.join("\n  ") + "\n")
  } else {
    console.log(`\n  -\n`);
  }

  console.log("IGNORED");
  if (ignored.length) {
    console.log("\n  " + ignored.join("\n  ") + "\n")
  } else {
    console.log(`\n  -\n`);
  }
}

async function init() {
  if (!fs.existsSync(CONFIG_FILENAME)) {
    console.log(`CREATING ${CONFIG_FILENAME}`);
    await writeFile(CONFIG_FILENAME, `const path = require("path");
module.exports = {
  // Config for postgres client. see: https://node-postgres.com/api/client
  pg: {
    // user: process.env.PGUSER || process.env.USER,
    // password: process.env.PGPASSWORD,
    // host: process.env.PGHOST,
    // database: process.env.PGDATABASE || process.env.USER,
    // port: process.env.PGPORT,
    // connectionString: "postgres://user:password@host:5432/database",
    // ssl?: any, // passed directly to node.TLSSocket, supports all tls.connect options
    // types?: any, // custom type parsers
    // statement_timeout?: number, // number of milliseconds before a statement in query will time out, default is no timeout
    // query_timeout?: number, // number of milliseconds before a query call will timeout, default is no timeout
    // application_name?: string, // The name of the application that created this Client instance
    // connectionTimeoutMillis?: number, // number of milliseconds to wait for connection, default is no timeout
    // idle_in_transaction_session_timeout?: number // number of milliseconds before terminating any session with an open idle transaction, default is no timeout
  },
  // ignored migrations, eg "foo" skips "foo/up.sql" and "foo/down.sql" or "foo.sql"
  ignore: [],
  // directory where all migrations are stored
  dir: path.join(__dirname, "migrations"),
  // whether to try to create a database or not
  createOnMigrate: !process.argv.includes("--no-create-on-migrate")
}`);
  } else {
    console.log(`${CONFIG_FILENAME} already exists`);
  }

  if (!fs.existsSync("migrations")) {
    console.log(`CREATING migrations/`);
    fs.mkdirSync("migrations", { recursive: true });
  } else {
    console.log(`migrations/ already exists`);
  }
}

function renderError({ internalQuery, internalPosition, where, message, hint }) {
  const queryWithArrow = insertArrowAtPosition(internalQuery, internalPosition, "  ");
  return `\n  ERROR: ${message}\n  HINT: ${hint}\n\n  ${where.replace(/\n/g, "\n  ")}\n\n${queryWithArrow}\n`;
}

function insertArrowAtPosition(multilineText, position, linePrefix = "") {
  const { lines } = multilineText
    .split(/\n/g)
    .reduce(({ sum, lines }, line) => {
      const len = line.length + 1;
      const pos = position - sum;
      lines.push({ content: line, start: sum, pos: pos > 0 && pos < len ? pos : null });
      return { sum: sum + len, lines };
    }, { sum: 0, lines: [] });

  return lines.map(({ content, pos }) => linePrefix + content +
    (pos == null ? "" : "\n" + linePrefix + new Array(pos).fill("").join("-") + "^")).join("\n");
}

// Source: https://stackoverflow.com/a/2802804
function naturalSort(ar, index) {
  var L = ar.length, i, who, next,
    isi = typeof index == 'number',
    rx = /(\.\d+)|(\d+(\.\d+)?)|([^\d.]+)|(\.(\D+|$))/g;
  function nSort(aa, bb) {
    var a = aa[0], b = bb[0], a1, b1, i = 0, n, L = a.length;
    while (i < L) {
      if (!b[i]) return 1;
      a1 = a[i];
      b1 = b[i++];
      if (a1 !== b1) {
        n = a1 - b1;
        if (!isNaN(n)) return n;
        return a1 > b1 ? 1 : -1;
      }
    }
    return b[i] != undefined ? -1 : 0;
  }
  for (i = 0; i < L; i++) {
    who = ar[i];
    next = isi ? ar[i][index] || '' : who;
    ar[i] = [String(next).toLowerCase().match(rx), who];
  }
  ar.sort(nSort);
  for (i = 0; i < L; i++) {
    ar[i] = ar[i][1];
  }
}

module.exports = {
  rollback, migrate, recreate, createdb, dropdb, init, config, help, list, CONFIG_FILENAME
}
