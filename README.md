# Nubra MCP Server

This repository is the main GitHub/source repository for the Nubra MCP server.

It is optimized for the smooth local Codex folder-based flow:

1. clone or download the repo
2. run the bootstrap script
3. open the folder in Codex
4. let Codex discover the MCP from `.mcp.json`
5. start using Nubra tools in chat

If you want the smoothest source-based experience with the full local feature set, this is the recommended path.

## What This Repo Provides

- authentication tools
- instrument lookup and ref-id resolution
- quotes and historical data
- portfolio, holdings, funds, and positions
- options analytics and risk helpers
- HTML and image report generation
- CSV export workflows
- indicator tooling in repo mode
- backtesting in repo mode
- UAT-only trading tools with strict guardrails

## Recommended GitHub Flow for Codex

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
- `.env` from `.env.example` if needed
- the local dependency environment for the repo flow

### 3. Fill in `.env`

Local repo users should edit `.env` and provide:

```env
PHONE=
MPIN=
NUBRA_ENV=UAT
NUBRA_DEFAULT_EXCHANGE=NSE
```

### 4. Open the repo folder in Codex

Open the cloned `nubra-mcp-server` folder directly in Codex.

Important:

- open the main repo folder
- avoid stale worktrees when testing the latest version

### 5. Reload the workspace

Codex should detect `.mcp.json` automatically.

That file launches `run_stdio.ps1`, so the user should not need to manually add the MCP if they are using the GitHub folder flow.

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

This GitHub/source flow keeps the full local stack, including indicator and backtest tooling.

## Repo Structure

- `server.py`: MCP entrypoint
- `config.py`: config loading and user-home support
- `nubra_client.py`: service logic
- `tools/`: tool registration modules
- `tests/`: test suite

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
- local package outputs

Local build outputs such as `dist/`, `build/`, and `*.egg-info/` are ignored.

