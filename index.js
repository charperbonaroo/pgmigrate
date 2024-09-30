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
  client.on("notice", (notice) => {
    console.log(notice.severity, notice.name, notice.code, notice.message, "FROM", notice.where);
  });
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
  return getMigrationInDirectory(fs.realpathSync(config.dir));
}

function getMigrationInDirectory(root) {
  const files = getFilesRecursively(root).filter((f) => f.endsWith(".sql"));

  const idfn = (f) => f.replace(/(?:\/up|\/down)?\.sql$/, "");
  const ids = files.map((f) => idfn(f)).filter((f, i, a) => a.indexOf(f) == i);
  naturalSort(ids);

  const unsafeIds = ids.filter((id) => !/^[a-z\d_][a-z\d_-]*(?:\/[a-z\d_][a-z\d_-]*)*$/.test(id));
  if (unsafeIds.length) {
    console.warn(`\n* WARNING: THESE MIGRATIONS HAVE UNSAFE NAMES:\n*`);
    console.warn(unsafeIds.map((id) => `*   - ${JSON.stringify(id)}`).join("\n"));
    console.warn(`*\n* LIMIT ID NAMES TO a-z (lowercase), 0-9, "/", "_" AND "-".\n*`);
    console.warn(`* "unsafe" migration names might result in undefined or `);
    console.warn(`* unexpected behavior. Or it might be fine. Who knows?`);
    console.warn(`* Better play it safe and stick with safe names.\n`);
  }

  const realpath = (f) => f ? path.join(root, f) : null;

  return ids.map((id) => ({
    id,
    up: realpath(files.find((f) => f == path.join(id, "up.sql") || f == `${id}.sql`)),
    down: realpath(files.find((f) => f == path.join(id, "down.sql")))
  }));
}

function getFilesRecursively(root, subdir = []) {
  const dir = fs.readdirSync(path.join(root, ...subdir), { withFileTypes: true });
  const acc = [];

  for (const dirent of dir) {
    if (dirent.isDirectory()) {
      acc.push(...getFilesRecursively(root, [...subdir, dirent.name]));
    } else {
      acc.push(path.join(...subdir, dirent.name));
    }
  }

  return acc;
}


async function getTests(config) {
  const realPath = fs.realpathSync(config.testDir);
  const testPaths = fs.readdirSync(realPath, { withFileTypes: true });
  const testMap = testPaths.map((value) => {
    if (value.isFile() && value.name.endsWith(".sql")) {
      return { id: value.name.replace(/.sql$/, ""), path: path.join(realPath, value.name) }
    } else {
      return null;
    }
  }).filter(_ => _).reduce((acc, value) => ({ ...acc, [value.id]: value }), {});
  const keys = Object.keys(testMap)
  naturalSort(keys);
  return keys.map((key) => testMap[key]);
}

function isIgnored(config, key) {
  return config.ignore && config.ignore.includes(key);
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
    if (!error.message.includes("Attempted to drop a database that does not exist")) {
      throw error;
    }
  }
  await migrate(config);
}

async function migrate(config, ...argv) {
  if (config.createOnMigrate !== false) {
    await createdb(config);
  }
  const migrations = (await getMigrations(config)).filter(({ id }) => !isIgnored(config, id));
  const client = await getClient(config);

  await ensureMigrationsTables(client);
  console.log(`START MIGRATE`);

  await client.query(`BEGIN`);

  let lastQuery = null;
  let lastFile = null;

  try {
    const doneMigrations = await getMigrationsDone(client);
    const doneMigrationIds = doneMigrations.rows.map(({ migration_id }) => migration_id);
    let missingMigrations = migrations.filter(({ id }) => !doneMigrationIds.includes(id));

    const argvIds = argv.filter((arg) => arg[0] != "-");
    if (argvIds.length) {
      console.log(`FILTERING MIGRATIONS BY IDs [${argvIds.join(",")}]`);
      missingMigrations = missingMigrations.filter(({ id }) => argvIds.includes(id));
    }

    if (missingMigrations.length > 0) {
      const insertResult = await client.query(`INSERT INTO public.cbpgm_migration_runs DEFAULT VALUES RETURNING id`);
      const migrationRunId = insertResult.rows[0].id;
      const realPath = fs.realpathSync(config.dir);

      for (const { id, up } of missingMigrations) {
        if (up) {
          lastFile = realPath;
          console.log(`RUNNING ${up.replace(realPath, "").substr(1)}`);
          const sql = await readFile(up, "utf-8");
          lastQuery = sql;
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

    throw new Error(renderError(error, lastQuery, lastFile));
  }

  await client.end();
}

async function rollback(config, ...argv) {
  if (argv.length === 0) {
    throw new Error(`Requires --last or specific migration IDs`)
  }

  console.log(`STARTING ROLLBACK`);
  const migrations = (await getMigrations(config)).filter(({ id }) => !isIgnored(config, id)).reverse();
  const client = await getClient(config);

  await ensureMigrationsTables(client);

  await client.query(`BEGIN`);

  let lastQuery = null;
  let lastFile = null;

  try {
    let rollbackIds = [], lastRunId;

    {
      const doneMigrations = await getMigrationsDone(client);
      if (argv.includes("--last")) {
        const lastRunResult = await client.query(`SELECT id FROM public.cbpgm_migration_runs ORDER BY created_at DESC LIMIT 1`);
        lastRunId = lastRunResult.rows.length > 0 ? lastRunResult.rows[0].id : null;
        rollbackIds.push(...doneMigrations.rows
          .filter(({ migration_run_id }) => migration_run_id == lastRunId)
          .map(({ migration_id }) => migration_id));
        console.log(`ATTEMPT TO ROLLBACK LAST, IDS [${rollbackIds.join(",")}]`);
      }
      const argvIds = argv.filter((arg) => arg[0] != "-");
      if (argvIds.length > 0) {
        rollbackIds.push(...doneMigrations.rows
          .filter(({ migration_id }) => argv.includes(migration_id))
          .map(({ migration_id }) => migration_id));
        console.log(`ATTEMPT TO ROLLBACK WITH IDS [${argv.join(",")}]`);
      }
    }

    const rollbackMigrations = migrations.filter(({ id }) => rollbackIds.includes(id));
    console.log(`FOUND [${rollbackMigrations.map((m) => m.id).join(",")}]`)

    const realPath = fs.realpathSync(config.dir);

    if (rollbackMigrations.length > 0) {
      for (const { id, down } of rollbackMigrations) {
        await client.query(`DELETE FROM public.cbpgm_migrations WHERE migration_id = $1`, [id]);
        if (down) {
          lastFile = down;
          console.log(`ROLLBACK ${down.replace(realPath, "").substr(1)}`);
          const sql = await readFile(down, "utf-8");
          lastQuery = sql;
          await runSqlFileContent(client, sql);
        } else {
          console.log(`SKIP ${id} (no down.sql)`)
        }
      }
      if (lastRunId) {
        await client.query(`DELETE FROM public.cbpgm_migration_runs WHERE id = $1`, [lastRunId]);
      }
      await client.query(`COMMIT`);
      console.log(`${rollbackMigrations.length} MIGRATIONS ROLLED BACK`);
    } else {
      console.log(`NO MIGRATIONS TO ROLLBACK`);
      await client.query(`ROLLBACK`);
    }
  } catch (error) {
    await client.query(`ROLLBACK`);
    throw new Error(renderError(error, lastQuery, lastFile));
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
  // directory where all tests are stored
  testDir: path.join(__dirname, "tests"),
  // whether to try to create a database or not
  createOnMigrate: !process.argv.includes("--no-create-on-migrate")
}`);
  } else {
    console.log(`${CONFIG_FILENAME} already exists`);
  }

  if (!fs.existsSync("tests")) {
    console.log(`CREATING tests/`);
    fs.mkdirSync("tests", { recursive: true });
  } else {
    console.log(`tests/ already exists`);
  }

  if (!fs.existsSync("migrations")) {
    console.log(`CREATING migrations/`);
    fs.mkdirSync("migrations", { recursive: true });
  } else {
    console.log(`migrations/ already exists`);
  }
}

async function test(config) {
  const client = await getClient(config);
  try {
    console.log(`START TEST`);

    const tests = await getTests(config);

    if (!tests.length) {
      throw new Error(`Found no tests`);
    }

    let okCount = 0;
    let errCount = 0;

    for (const test of tests) {
      try {
        await client.query(`BEGIN`);

        const sql = await readFile(test.path, "utf-8");
        lastFile = test.path;
        lastQuery = sql;
        await runSqlFileContent(client, sql);

        test.ok = true;
        okCount++;
      } catch (error) {
        test.ok = false;
        errCount++;
        test.err = error && error.message;
        console.error(renderError(error, lastQuery, lastFile));
      } finally {
        await client.query(`ROLLBACK`);
      }
      console.log(`TEST ${test.id}: ${test.ok ? "OK" : test.err}`);
    }

    if (errCount > 0) {
      throw new Error(`${errCount} / ${tests.length} FAILED`);
    }

    console.log(`${tests.length} OK!`);
  } finally {
    await client.end();
  }
}

function renderError(error, lastQuery, lastFile) {
  const { internalQuery, internalPosition, where, message, hint, position } = error;
  if (internalQuery) {
    const queryWithArrow = insertArrowAtPosition(internalQuery, internalPosition, "  ");
    return `\n  ERROR: ${message}\n  FILE: ${lastFile}\n  HINT: ${hint || "-"}\n\n  ${(where || "").replace(/\n/g, "\n  ")}\n\n${queryWithArrow}\n`;
  } else if (lastQuery && position) {
    const queryWithArrow = insertArrowAtPosition(lastQuery, parseFloat(position), "  ");
    return `\n  ERROR: ${message}\n  FILE: ${lastFile}\n  HINT: ${hint || "-"}\n\n  ${queryWithArrow}\n`;
  } else {
    return require("util").inspect(error, false, Infinity, true);
  }
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
  rollback,
  down: rollback,
  migrate,
  up: migrate,
  recreate,
  createdb,
  create: createdb,
  dropdb,
  drop: dropdb,
  init,
  config,
  help,
  list,
  test,
  CONFIG_FILENAME
}
