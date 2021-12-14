#!/usr/bin/env -S node --require ./.pnp.cjs

const migrate = require(".");
const path = require("path");
const fs = require("fs");

const cmd = process.argv[process.argv.length - 1];
const configPath = path.join(process.cwd(), migrate.CONFIG_FILENAME);

const config = fs.existsSync(configPath) ? require(configPath) : {
  dir: path.join(process.cwd(), "migrations"),
};

config.pg = config.pg || {};

async function main(cmd) {
  try {
    if (!migrate[cmd]) {
      if (cmd) {
        console.log(`Unknown command '${cmd}'?\n`);
      }
      cmd = "help";
    }
    await migrate[cmd](config);
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

main(cmd);
