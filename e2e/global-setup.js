const { execSync } = require('child_process');
const axios = require('axios');
const path = require('path');

const COMPOSE_FILE = path.join(__dirname, 'docker-compose.test.yml');
const API_URL = 'http://localhost:18000/api/health';
const FRONTEND_URL = 'http://localhost:13000';
const MAX_WAIT_MS = 180_000;
const POLL_INTERVAL_MS = 3_000;

async function waitForService(url, label) {
  const start = Date.now();
  while (Date.now() - start < MAX_WAIT_MS) {
    try {
      const res = await axios.get(url, { timeout: 3000 });
      if (res.status >= 200 && res.status < 400) {
        console.log(`  [OK] ${label} is ready`);
        return;
      }
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
  throw new Error(`Timed out waiting for ${label} at ${url}`);
}

module.exports = async function globalSetup() {
  console.log('\n--- E2E Global Setup ---');
  console.log('Starting docker compose stack...');

  execSync(`docker compose -f ${COMPOSE_FILE} up -d --build --wait`, {
    stdio: 'inherit',
    timeout: MAX_WAIT_MS,
  });

  console.log('Waiting for services to become healthy...');
  await waitForService(API_URL, 'Backend API');
  await waitForService(FRONTEND_URL, 'Frontend');

  // Brief additional wait for MQTT subscription to establish
  await new Promise((r) => setTimeout(r, 2000));
  console.log('--- E2E Global Setup Complete ---\n');
};
