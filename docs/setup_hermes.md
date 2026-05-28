# Hermes Remote Setup

This guide sets up Hermes Agent on a fresh Ubuntu/Debian remote machine, backed by local Ollama `gemma4`, with Hermes' API Server enabled for the EdgeCitadel bridge adapter.

This covers only the remote-machine Hermes runtime setup. After this succeeds, configure `adapters/hermes/agent.env` with the aggregator host, NATS token, and Hermes bearer token, then start `python -m adapters.hermes.adapter`.

## Assumptions

- You are on the remote machine over SSH.
- The machine is Ubuntu/Debian based.
- The machine has enough RAM/VRAM for local `gemma4`.
- You will run commands as a sudo-capable user, not as root.

## 1. Install base packages

```bash
sudo apt-get update
sudo apt-get install -y \
  curl \
  git \
  ca-certificates \
  build-essential \
  python3 \
  python3-venv \
  python3-pip \
  jq
```

## 2. Install Ollama

```bash
curl -fsSL https://ollama.com/install.sh | sh
sudo systemctl enable --now ollama
```

Verify Ollama:

```bash
ollama --version
curl http://127.0.0.1:11434/api/version
```

## 3. Pull and warm up Gemma 4

```bash
ollama pull gemma4
ollama run gemma4 "Reply with: ready"
```

Expected result: Ollama downloads the model, then replies to the smoke prompt.

## 4. Install Hermes Agent

```bash
curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash
source ~/.bashrc
```

Verify Hermes:

```bash
hermes --version
hermes doctor
```

If `hermes` is not found, open a new SSH shell and retry:

```bash
hermes --version
```

## 5. Point Hermes at local Ollama

Run the interactive model setup:

```bash
hermes model
```

Choose these values in the wizard:

```text
Provider: Custom endpoint
API base URL: http://127.0.0.1:11434/v1
API key: leave blank
Model: gemma4
Context length: leave blank unless you know the target value
```

Smoke test Hermes against local Ollama:

```bash
hermes "Reply with exactly: hermes-local-ready"
```

Expected result: Hermes replies with `hermes-local-ready` or a very close equivalent.

## 6. Enable the Hermes API Server gateway

Run the gateway setup:

```bash
hermes gateway setup
```

In the wizard:

```text
Enable API Server platform: yes
Port: 8642
```

Start the gateway in the foreground once:

```bash
hermes gateway run
```

Copy the bearer token printed by Hermes. This is the value for `HERMES_TOKEN` in `adapters/hermes/agent.env`.

In a second SSH session, verify the API Server:

```bash
export HERMES_TOKEN='<token-printed-by-hermes-gateway>'
curl -s \
  -H "Authorization: Bearer ${HERMES_TOKEN}" \
  http://127.0.0.1:8642/v1/models | jq .
```

Expected result: a JSON model list, including the active Ollama-backed model.

## 7. Install the Hermes gateway as a user service

Stop the foreground `hermes gateway run` process with `Ctrl+C`, then install the gateway service:

```bash
hermes gateway install
systemctl --user daemon-reload
```

List the installed Hermes service name:

```bash
systemctl --user list-unit-files | grep -i hermes
```

If the service is named `ai.hermes.gateway.service`, enable and start it:

```bash
systemctl --user enable --now ai.hermes.gateway.service
systemctl --user status ai.hermes.gateway.service
```

If the listed service name is different, replace `ai.hermes.gateway.service` with the actual name:

```bash
systemctl --user enable --now <actual-hermes-service-name>
systemctl --user status <actual-hermes-service-name>
```

Enable user services to start after reboot:

```bash
sudo loginctl enable-linger "$USER"
```

Verify the gateway still answers after service install:

```bash
curl -s \
  -H "Authorization: Bearer ${HERMES_TOKEN}" \
  http://127.0.0.1:8642/v1/models | jq .
```

View logs:

```bash
journalctl --user -u ai.hermes.gateway.service -n 100 -f
```

If your service name is different, replace `ai.hermes.gateway.service` in the log command.

## Codex Prompt For The Remote Machine

After installing Codex and cloning this repo on the remote machine, use this prompt:

```text
Follow docs/setup_hermes.md exactly on this fresh remote machine.
Run the commands needed to install base packages, Ollama, local gemma4, Hermes Agent, configure Hermes to use Ollama at http://127.0.0.1:11434/v1, enable the Hermes API Server on port 8642, and install the Hermes gateway as a user service.
Stop after the Hermes API Server responds successfully at http://127.0.0.1:8642/v1/models and report the HERMES_TOKEN location or the token value printed by Hermes gateway.
Do not configure the EdgeCitadel NATS bridge yet.
```

## Troubleshooting

If Ollama is not reachable:

```bash
sudo systemctl status ollama
sudo systemctl restart ollama
curl http://127.0.0.1:11434/api/version
```

If Hermes cannot see the model:

```bash
ollama list
curl -s http://127.0.0.1:11434/v1/models | jq .
hermes model
```

If the Hermes gateway is not reachable:

```bash
systemctl --user status ai.hermes.gateway.service
journalctl --user -u ai.hermes.gateway.service -n 100
hermes gateway run
```
