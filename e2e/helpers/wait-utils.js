/**
 * Sleep for the given number of milliseconds.
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Poll a function until it returns a truthy value or times out.
 * @param {Function} fn - Async function to poll. Should return a truthy value on success.
 * @param {object} opts
 * @param {number} opts.timeout - Max time to wait in ms (default 15000)
 * @param {number} opts.interval - Poll interval in ms (default 500)
 * @param {string} opts.label - Label for error message
 * @returns {Promise<*>} The truthy value returned by fn
 */
async function pollUntil(fn, { timeout = 15_000, interval = 500, label = 'condition' } = {}) {
  const start = Date.now();
  let lastError;
  while (Date.now() - start < timeout) {
    try {
      const result = await fn();
      if (result) return result;
    } catch (err) {
      lastError = err;
    }
    await sleep(interval);
  }
  throw new Error(`pollUntil timed out waiting for: ${label}${lastError ? ` (last error: ${lastError.message})` : ''}`);
}

/**
 * Wait for an MQTT-published event to propagate through the pipeline:
 * MQTT -> Backend -> DB -> API
 *
 * @param {Function} publishFn - Async function that publishes the MQTT message
 * @param {Function} verifyFn - Async function that checks the API/DB result; should return truthy on success
 * @param {object} opts
 */
async function waitForPipeline(publishFn, verifyFn, { timeout = 15_000, interval = 500, label = 'pipeline' } = {}) {
  await publishFn();
  // Small delay for MQTT -> backend processing
  await sleep(300);
  return pollUntil(verifyFn, { timeout, interval, label });
}

module.exports = { sleep, pollUntil, waitForPipeline };
