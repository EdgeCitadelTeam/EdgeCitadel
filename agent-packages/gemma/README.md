# Gemma Managed Agent

Gemma is a minimal EdgeCitadel Managed Agent that sends one chat prompt to a
local Ollama service and returns the response. It deliberately has no memory,
tool loop, streaming layer, or task-specific skills.

Install Ollama, pull the small default model, then install the Agent Package:

```bash
ollama pull gemma3:1b
edgecitadel agent install gemma
```

Set `OLLAMA_HOST`, `OLLAMA_PORT`, `OLLAMA_MODEL`, or `OLLAMA_TIMEOUT_SEC` before
starting the Agent to override the local defaults. The package has no additional
Python dependencies, so EdgeCitadel runs it in the shared Managed Agent runtime.

Runtime tests live in `agent-runtime/tests/gemma_runtime/`.
