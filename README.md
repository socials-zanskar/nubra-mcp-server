# Nubra Base MCP

Minimal Python MCP server for Nubra built on `FastMCP`.

This base server is meant to give you a clean starting point:

- authentication tools
- instrument lookup tools
- quote and historical-data tools
- option-chain tools
- portfolio holdings and funds snapshots
- read-only analytics and TA-Lib signal scans
- read-only order, margin, and position tools

## Files

- `server.py`: MCP entrypoint
- `config.py`: environment-driven settings
- `nubra_client.py`: Nubra REST/SDK wrapper and service layer
- `tools/`: MCP tool registration modules

## Setup

1. Bootstrap the repo:

```powershell
.\bootstrap.ps1
```

2. Fill in `.env`.

This repo auto-loads `.env` from the repo root, so local MCP clients do not need you to pre-export environment variables in the shell.

Required for interactive login:

- `PHONE`
- `MPIN`

## Run

Recommended for local MCP clients:

```powershell
.\run_stdio.ps1
```

Recommended for local HTTP testing:

```powershell
.\run_http.ps1
```

HTTP transport:

```powershell
python server.py --transport streamable-http
```

stdio transport:

```powershell
python server.py --transport stdio
```

SSE transport:

```powershell
python server.py --transport sse
```

Default local endpoints:

- health: `http://127.0.0.1:8000/health`
- mcp: `http://127.0.0.1:8000/mcp`

## MCP Client Integration

Project-local MCP config is included in `.mcp.json`.

That lets repo-aware local clients such as Claude Code discover the MCP from the repo folder with minimal setup.

For clients that want an explicit config file, use `mcp-client-config.example.json`.

Typical local flow:

1. Open the repo folder.
2. Run `.\bootstrap.ps1` once.
3. Fill in `.env`.
4. Let the client launch `.\run_stdio.ps1` via `.mcp.json`, or register `http://127.0.0.1:8000/mcp` after starting `.\run_http.ps1`.

## Notes

- Start with `NUBRA_ENV=UAT` while validating the tool surface.
- Authentication uses only the OTP -> MPIN flow. The saved session token is reused until Nubra rejects it or you log out.
- Agents should call `auth_status` first. If `requires_login` is true, they should ask for phone number, call `begin_auth_flow`, then ask for OTP, then MPIN, and then resume the original task.
- Trading is explicitly disabled in this MCP. Order placement, strategy execution, cancellation, and square-off are blocked in both the tool layer and service layer.
- TA-Lib scans require `pandas` and `TA-Lib` to be installed.
- The server already exposes more than a bare minimum tool set, but the structure is suitable as a base MCP to extend.
