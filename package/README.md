# nubra-mcp

Install Nubra MCP from PyPI and connect it to Codex.

This package is the lighter distribution path for users who do not want to clone the full GitHub repo.

Note for maintainers:

- this folder contains the package-focused README and PyPI-facing documentation
- the actual publishable build metadata currently remains at the repo root in [pyproject.toml](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\pyproject.toml)
- that keeps the build stable while the repo and package source trees are still shared

## Install

Base install:

```powershell
pip install nubra-mcp
```

Optional extras:

```powershell
pip install "nubra-mcp[indicators]"
pip install "nubra-mcp[backtest]"
pip install "nubra-mcp[full]"
```

What the base install includes:

- auth flow
- quotes
- historical data
- portfolio tools
- exports and reports
- UAT-only trading tools

What is optional:

- indicator tooling via `nubra-talib`
- backtesting via `vectorbt`

## Codex CLI Setup

1. Install the package:

```powershell
pip install nubra-mcp
```

2. Register the MCP in Codex CLI:

```powershell
codex mcp add nubra-mcp -- nubra-mcp
```

If `nubra-mcp` is not on PATH, use the full executable path instead.

3. Verify:

```powershell
codex mcp list
```

4. Start Codex:

```powershell
codex
```

5. In chat:

- `Use nubra mcp and check auth status`

## Codex Desktop App Setup

1. Install the package:

```powershell
pip install nubra-mcp
```

2. Add a global MCP server entry in Codex desktop:

- name: `nubra-mcp`
- type: `stdio`
- command: `nubra-mcp`

3. Reload or restart Codex desktop.

4. In chat:

- `Use nubra mcp and check auth status`

## Credentials and Login

`nubra-mcp init` is optional.

Users can log in interactively through chat:

1. call `auth_status`
2. enter phone number
3. enter OTP
4. enter MPIN

Optional helper:

```powershell
nubra-mcp init
```

That creates a local config template under the user home directory.

## UAT Trading

Trading actions are UAT-only.

Recommended flow:

1. `Preview a UAT order for RELIANCE with quantity 1, limit price 1500, and CNC`
2. review the preview table
3. confirm
4. `Place the same UAT order`

## Troubleshooting

If Codex already has an older local Nubra MCP configured, remove it first so only one Nubra MCP stays active.

CLI example:

```powershell
codex mcp list
codex mcp remove nubra
codex mcp add nubra-mcp -- nubra-mcp
```

If the command exists but Codex cannot find it, use the full executable path instead of just `nubra-mcp`.

## Source Repo

GitHub/source users who want the full repo flow should use the main repository instead:

[https://github.com/socials-zanskar/nubra-mcp-server](https://github.com/socials-zanskar/nubra-mcp-server)
