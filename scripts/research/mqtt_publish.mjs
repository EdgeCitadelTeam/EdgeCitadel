import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

let mqtt;
try {
  mqtt = require("../../e2e/node_modules/mqtt");
} catch {
  mqtt = require("mqtt");
}

function parseArgs(argv) {
  const parsed = {};
  for (let i = 0; i < argv.length; i += 2) {
    parsed[argv[i].replace(/^--/, "")] = argv[i + 1];
  }
  return parsed;
}

const args = parseArgs(process.argv.slice(2));
if (!args.url || !args.topic || args.payload === undefined) {
  console.error("usage: mqtt_publish.mjs --url <url> --topic <topic> --payload <payload>");
  process.exit(2);
}

const options = { protocolVersion: 4, clean: true };
if (args.username) options.username = args.username;
if (args.password) options.password = args.password;
const client = mqtt.connect(args.url, options);

client.on("connect", () => {
  client.publish(args.topic, args.payload, { retain: false, qos: 0 }, (error) => {
    if (error) {
      console.error(error.message);
      process.exitCode = 1;
    }
    client.end(false, () => process.exit(process.exitCode ?? 0));
  });
});

client.on("error", (error) => {
  console.error(error.message);
  process.exit(1);
});
