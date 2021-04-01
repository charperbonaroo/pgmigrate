#!/usr/bin/env node

const migrate = require(".");
const path = require("path");
const fs = require("fs");

const cmd = process.argv[process.argv.length - 1];

const configPath = path.join(process.cwd(), ".pgconfig.js");

const config = fs.existsSync(configPath) ? require(configPath) : {
  dir: path.join(process.cwd(), "migrations"),
};

if (!fs.existsSync(config.dir)) {
  console.log(`CREATING ${config.dir} FOR MIGRATIONS`);
  fs.mkdirSync(config.dir, { recursive: true });
}

config.pg = config.pg || {};

config.pg.database = config.pg.database || process.env.PGDATABASE;

if (!config.pg.database) {
  throw new Error(`Couldn't determine current database - create .pgconfig.js config file or set env PGDATABASE`);
}

async function main(cmd) {
  try {
    if (!migrate[cmd]) {
      cmd = "migrate";
    }
    migrate[cmd](config);
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

main(cmd);
