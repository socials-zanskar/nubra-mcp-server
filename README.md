# Nubra MCP Server

This is the main GitHub repository for the Nubra MCP server.

It is optimized for the smooth local developer and power-user flow:

1. clone or download the repo
2. run the bootstrap script
3. open the folder in Codex
4. let Codex discover the MCP from [`.mcp.json`](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\.mcp.json)
5. start using Nubra tools in chat

If you want the easiest source-based setup with full local features, this is the recommended path.

## What This Repo Provides

- authentication tools
- instrument lookup and ref-id resolution
- quotes and historical data
- portfolio, holdings, funds, and positions
- options analytics and risk helpers
- HTML/image report generation
- CSV export workflows
- `nubra-talib` indicator tooling in repo mode
- `vectorbt` backtesting in repo mode
- UAT-only trading tools with strict guardrails

## Two User Paths

### 1. GitHub / source flow

Best for:

- developers
- contributors
- users who want the smoothest Codex folder-based experience
- users who want the full local feature set, including repo-installed optional tooling

### 2. PyPI / package flow

Best for:

- users who want a lighter package install
- users who want to register Nubra MCP globally in Codex

The PyPI/package build now lives under:

- [package](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\package)

Its package-specific instructions are in:

- [package/README.md](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\package\README.md)

## Recommended GitHub Flow for Codex

This is the flow you said you want to keep smooth for users who download from GitHub.

### 1. Clone the repo

```powershell
git clone https://github.com/socials-zanskar/nubra-mcp-server.git
cd nubra-mcp-server
```

### 2. Bootstrap the repo

```powershell
powershell -ExecutionPolicy Bypass -File .\bootstrap.ps1
```

This creates:

- `.venv`
- `.env` from [`.env.example`](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\.env.example) if needed
- the local dependency environment for the repo flow

### 3. Fill in `.env`

Local repo users should edit:

- [`.env`](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\.env)

Typical values:

```env
PHONE=
MPIN=
NUBRA_ENV=UAT
NUBRA_DEFAULT_EXCHANGE=NSE
```

### 4. Open the repo folder in Codex

Open this folder directly in Codex:

- [nubra-mcp-server](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server)

Important:

- open the main repo folder
- avoid stale worktrees when testing the latest version

### 5. Reload the workspace

Codex should detect:

- [`.mcp.json`](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\.mcp.json)

That file launches:

- [run_stdio.ps1](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\run_stdio.ps1)

So the user should not need to manually add the MCP if they are using the GitHub folder flow.

### 6. Start using the MCP

Example first prompt:

- `Use nubra mcp and check auth status`

If login is required, the intended flow is:

1. phone number
2. OTP
3. MPIN

## Why the GitHub Folder Flow Is Smooth

This repo keeps the Codex folder-based experience intentionally simple:

- no global registration is required for repo users
- `.mcp.json` lives in the repo
- `run_stdio.ps1` is already wired
- `bootstrap.ps1` prepares the environment

That makes it feel much closer to:

- open folder
- connect MCP
- use it

## UAT Trading Guardrails

Trading actions are enabled only in `UAT`.

Allowed in `UAT`:

- `preview_uat_order`
- `place_uat_order`
- `modify_uat_order`
- `cancel_uat_order`
- `square_off_uat_position`
- `place_uat_options_strategy`
- `place_uat_named_option_strategy`

Blocked in `PROD`:

- all order placement
- all modify/cancel actions
- all square-off actions
- all strategy execution actions

Recommended flow:

1. preview the order
2. show the order preview table
3. confirm
4. place the order

## Repo-Mode Optional Tooling

The GitHub/source flow keeps the full local stack:

- `nubra-talib` for indicator tools
- `vectorbt` for backtests

That means the repo flow is still the best experience if the user wants every feature available locally.

## Repo Structure

- [server.py](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\server.py): MCP entrypoint
- [config.py](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\config.py): config loading and user-home support
- [nubra_client.py](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\nubra_client.py): service logic
- [tools](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\tools): tool registration modules
- [tests](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\tests): test suite
- [package](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\package): PyPI/package distribution files

## Local Validation

Import smoke test:

```powershell
.\.venv\Scripts\python.exe -c "import server; print('server-import-ok')"
```

Run tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## Public Safety Notes

Do not commit:

- `.env`
- `auth_state.json`
- `artifacts/`
- build outputs
- package outputs

Local build outputs such as `dist/`, `build/`, and `*.egg-info/` are ignored.

## Publishing

This repo is the main source repository.

The PyPI package is built from:

- [package](C:\Users\suboth sundar\Desktop\Nubra_API_Full_context\nubra-mcp-server\package)

That separation keeps:

- the GitHub/source flow full-featured
- the PyPI install path lighter and cleaner
