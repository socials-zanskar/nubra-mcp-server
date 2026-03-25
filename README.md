# Nubra MCP Server

This repository is the main GitHub/source repository for the Nubra MCP server.

It provides authentication, instrument lookup, quotes, historical data, options analytics, portfolio and account tools, report generation, screening utilities, backtesting, and order placement through `UAT`.

## Setup and Start

Follow the steps below to run the MCP locally.

### 1. Prerequisites

Ensure the following are available:

- Python 3.11
- PowerShell
- network access to install Python dependencies

### 2. Clone the repository

```powershell
git clone https://github.com/socials-zanskar/nubra-mcp-server.git
cd nubra-mcp-server
```

### 3. Bootstrap the environment

```powershell
powershell -ExecutionPolicy Bypass -File .\bootstrap.ps1
```

This script performs the following actions:

- creates `.venv` if it does not exist
- upgrades `pip`
- installs packages from `requirements.txt`
- creates `.env` from `.env.example` if `.env` is missing

After this step, the repository should contain:

- `.venv`
- `.env`

### 4. Configure `.env`

Edit `.env` and set the required values:

```env
PHONE=
MPIN=
NUBRA_ENV=UAT
NUBRA_DEFAULT_EXCHANGE=NSE
LOG_LEVEL=INFO
HOST=127.0.0.1
PORT=8000
MCP_PATH=/mcp
AUTH_STATE_FILE=auth_state.json
```

Recommended settings:

- start with `NUBRA_ENV=UAT`
- keep `HOST=127.0.0.1`
- keep `MCP_PATH=/mcp` unless you need a custom path

### 5. Start the MCP

For MCP clients that use stdio:

```powershell
.\run_stdio.ps1
```

For local HTTP testing:

```powershell
.\run_http.ps1
```

Equivalent direct commands:

```powershell
python server.py --transport stdio
python server.py --transport streamable-http
python server.py --transport sse
```

### 6. Verify the server

When using HTTP transport, the default local endpoints are:

- health: `http://127.0.0.1:8000/health`
- mcp: `http://127.0.0.1:8000/mcp`

Import smoke test:

```powershell
.\.venv\Scripts\python.exe -c "import server; print('server-import-ok')"
```

Run tests:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## MCP Client Integration

This repository includes project-local MCP configuration in `.mcp.json`.

Standard repo-based setup:

1. Open the `nubra-mcp-server` folder in your MCP client.
2. Run `.\bootstrap.ps1`.
3. Fill in `.env`.
4. Allow the client to start `.\run_stdio.ps1` through `.mcp.json`.

For clients that require manual registration, use one of the following:

- `mcp-client-config.example.json`
- or the HTTP endpoint `http://127.0.0.1:8000/mcp` after starting `.\run_http.ps1`

## Authentication

Protected tools should use the following flow:

1. call `auth_status`
2. if login is required, ask for phone number
3. call `begin_auth_flow`
4. ask for OTP and call `verify_otp`
5. ask for MPIN and call `verify_mpin`
6. continue with the original request

Authentication uses the OTP -> MPIN flow.

## What This Repo Provides

The server includes tools for:

- authentication and session handling
- instrument search and ref-id resolution
- quotes and historical market data
- option chain analytics and Greeks
- funds, holdings, positions, and portfolio summaries
- HTML and image report generation
- CSV export workflows
- screening and indicator-based tooling
- backtesting utilities
- order placement through `UAT`

For `PROD`, order placement is temporarily blocked. Other supported operations continue to work.

## Repo Structure

- `server.py`: MCP entrypoint
- `config.py`: configuration loading
- `nubra_client.py`: Nubra client and service logic
- `tools/`: MCP tool registration modules
- `tests/`: test suite
- `bootstrap.ps1`: setup script
- `run_stdio.ps1`: stdio launcher
- `run_http.ps1`: HTTP launcher

## First Use

Common tasks after startup include:

- check authentication status
- search for an instrument
- get the current price for a symbol
- fetch historical data
- review option chain data
- generate a portfolio report

Example first prompt:

- `Use nubra mcp and check auth status`

## Local Notes

- start with `UAT` when validating setup and workflows
- keep `.env`, auth state files, and local artifacts out of source control
- build outputs such as `dist/`, `build/`, and `*.egg-info/` should remain uncommitted
