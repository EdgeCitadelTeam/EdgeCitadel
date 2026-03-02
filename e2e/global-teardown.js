const { execSync } = require('child_process');
const path = require('path');

const COMPOSE_FILE = path.join(__dirname, 'docker-compose.test.yml');

module.exports = async function globalTeardown() {
  console.log('\n--- E2E Global Teardown ---');
  try {
    execSync(`docker compose -f ${COMPOSE_FILE} down -v --remove-orphans`, {
      stdio: 'inherit',
      timeout: 60_000,
    });
    console.log('Docker compose stack torn down.');
  } catch (err) {
    console.error('Teardown error:', err.message);
  }
  console.log('--- E2E Global Teardown Complete ---\n');
};
