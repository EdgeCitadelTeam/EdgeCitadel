# Gemma Managed Agent

Gemma is a complete EdgeCitadel-managed Agent backed by a local Ollama service. It
provides reasoning, summarization, classification, and code-explanation skills.

Install it after joining an Edge:

```bash
edgecitadel agent install gemma
```

The Agent service validates `plugin.yaml` and `plugin.lock.json`, creates a
private runtime from `edgecitadel_gemma_plugin/requirements.txt`, and supervises
the process. Gemma talks only to the private agentd connector API and never
receives NATS or Leaf credentials. Configure Ollama with `OLLAMA_HOST`,
`OLLAMA_PORT`, and `OLLAMA_MODEL` before starting the Agent when the defaults are
not suitable.

Runtime tests live in `plugin-toolkit/tests/gemma_runtime/`.
