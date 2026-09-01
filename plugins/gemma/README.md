# Gemma Plugin

Gemma is an installable EdgeCitadel Plugin backed by a local Ollama service. It
provides reasoning, summarization, classification, and code-explanation skills.

Install it after joining an Edge:

```bash
edgecitadel plugin install gemma
```

The Supervisor validates `plugin.yaml` and `plugin.lock.json`, creates a private
runtime from `edgecitadel_gemma_plugin/requirements.txt`, and supplies the NATS
endpoint selected by the Edge's messaging mode. Configure Ollama with
`OLLAMA_HOST`, `OLLAMA_PORT`, and `OLLAMA_MODEL` before starting the Plugin when
the defaults are not suitable.

Runtime tests live in `plugin-toolkit/tests/gemma_runtime/`.
