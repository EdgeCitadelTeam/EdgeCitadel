---
name: shell-exec
description: Execute an operator-approved shell command on the Edge host and return bounded stdout, stderr, and exit status.
compatibility: EdgeCitadel plugin runtime v1.
metadata:
  version: "0.1.0"
---

# Shell execution

Use only for commands the operator has explicitly authorized. The runtime caps
execution time and response size, but the command runs with the plugin process's
host permissions.
