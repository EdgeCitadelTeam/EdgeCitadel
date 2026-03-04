const { chromium } = require('playwright');
const { spawn } = require('child_process');
const path = require('path');

const DASHBOARD_URL = 'http://localhost:3000';
const VIDEO_DIR = path.resolve(__dirname, '..', 'docs');

async function main() {
  console.log('Launching browser...');
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    recordVideo: { dir: VIDEO_DIR, size: { width: 1440, height: 900 } },
  });

  const page = await context.newPage();

  // Load the dashboard and wait for it to settle
  console.log('Loading dashboard...');
  await page.goto(DASHBOARD_URL, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);

  // Start the MQTT simulation in the background
  console.log('Starting MQTT simulation...');
  const sim = spawn('python3', [
    path.resolve(__dirname, 'simulate_conversation.py'),
  ]);
  sim.stdout.on('data', (d) => process.stdout.write(`  [sim] ${d}`));
  sim.stderr.on('data', (d) => process.stderr.write(`  [sim-err] ${d}`));

  // Wait for messages to start appearing
  await page.waitForTimeout(4000);

  // Scroll down slowly to show messages coming in
  console.log('Scrolling through messages...');
  const chatArea = page.locator('.overflow-y-auto').first();
  await chatArea.evaluate((el) => el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' }));
  await page.waitForTimeout(2000);

  // Click on an agent in the sidebar to filter
  console.log('Selecting openclaw agent...');
  const openclawCard = page.locator('text=OpenClaw Edge').first();
  if (await openclawCard.isVisible()) {
    await openclawCard.click();
    await page.waitForTimeout(2500);
  }

  // Click "All Agents" to go back to full view
  console.log('Back to all agents...');
  const allAgents = page.locator('text=All Agents').first();
  if (await allAgents.isVisible()) {
    await allAgents.click();
    await page.waitForTimeout(2000);
  }

  // Click on a message with correlation_id to show the trace panel
  console.log('Opening conversation trace...');
  const messageBubbles = page.locator('[class*="rounded-lg border"]').filter({ hasText: 'macmini-backend' });
  const count = await messageBubbles.count();
  if (count > 0) {
    await messageBubbles.first().click();
    await page.waitForTimeout(3000);
  }

  // Close trace panel by clicking the message again
  if (count > 0) {
    await messageBubbles.first().click();
    await page.waitForTimeout(1000);
  }

  // Switch to Flow tab
  console.log('Switching to Flow tab...');
  const flowTab = page.locator('button', { hasText: 'Flow' });
  if (await flowTab.isVisible()) {
    await flowTab.click();
    await page.waitForTimeout(3000);
  }

  // Switch to Logs tab
  console.log('Switching to Logs tab...');
  const logsTab = page.locator('button', { hasText: 'Logs' });
  if (await logsTab.isVisible()) {
    await logsTab.click();
    await page.waitForTimeout(2000);
  }

  // Back to Chat
  console.log('Back to Chat...');
  const chatTab = page.locator('button', { hasText: 'Chat' });
  if (await chatTab.isVisible()) {
    await chatTab.click();
    await page.waitForTimeout(2000);
  }

  // Final scroll to bottom
  await chatArea.evaluate((el) => el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' }));
  await page.waitForTimeout(1500);

  // Stop
  console.log('Stopping recording...');
  sim.kill();
  await page.close();
  await context.close();

  // Get the video path
  const video = page.video();
  if (video) {
    const videoPath = await video.path();
    console.log(`Video saved: ${videoPath}`);
  }

  await browser.close();
  console.log('Done! Video saved to docs/ directory.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
