# Nubra MCP Webinar Prompt Sheet

## Goal

Use this sheet in a webinar to show how to build an MCP server on top of the Nubra API docs:

- Docs source: https://nubra.io/products/api/docs/index.html
- Outcome: an MCP that can authenticate, fetch quotes, get current price, calculate yesterday change, fetch historical data, work with option chains and Greeks, estimate margin, resolve instruments, and search index master data.

This sheet is written so you can give the prompts to a coding agent step by step and end up with something close to the current `nubra-mcp-server`.

---

## What This MCP Is Built On

Use this section when you want users to reproduce the same implementation style.

- Language: Python
- Runtime: Python 3.11 or newer
- MCP framework: `FastMCP` from the Python MCP server package
- Web server: `FastAPI` + `uvicorn`
- HTTP client: `requests`
- Validation/models: `pydantic`
- OTP support: `pyotp`
- Transport modes:
  - `stdio`
  - `streamable-http`
  - optionally `sse`

Suggested project structure:

```text
nubra-mcp-server/
  server.py
  config.py
  nubra_client.py
  requirements.txt
  tools/
    auth.py
    quotes.py
    options.py
    analytics.py
    orders.py
```

Suggested `requirements.txt` contents:

```text
fastapi
uvicorn
requests
pydantic
pyotp
mcp
```

Recommended environment/config inputs:

- `PHONE`
- `MPIN`
- `TOTP_SECRET`
- `NUBRA_ENV`
- `HOST`
- `PORT`
- `MCP_PATH`
- `NUBRA_DEFAULT_EXCHANGE`
- auth state file path if you want persistent login state

Implementation style:

- `server.py`: bootstraps FastMCP and registers tools
- `nubra_client.py`: contains both:
  - low-level Nubra REST client
  - high-level service methods used by MCP tools
- `tools/*.py`: groups MCP tools by domain
- auth/session state is persisted locally so the MCP can survive restarts
- prices are normalized from paise-like units into rupees for human-friendly responses where appropriate

---

## What The Final MCP Should Do

The MCP should support:

- Nubra auth: OTP, MPIN, auto-login, logout, auth status, environment switching
- Quotes by symbol
- Current price by symbol
- Yesterday / previous-close change for stock, index, or option
- Historical data lookup
- Option chain lookup
- Option Greeks snapshot
- Delta-neutral pair search
- Intraday option Greek-change analysis
- Margin estimation
- ATM straddle margin estimation for symbols like `NIFTY`, `BANKNIFTY`, `RELIANCE`, `INFY`, `TCS`
- Instrument lookup by symbol and exchange
- Ref ID, tick size, lot size, nubra name lookup
- Index master lookup using the public CSV
- Fuzzy matching for index names like `India VIX`, `INDIAVIX`, `VIX`

---

## Build Rules For The Agent

Use these rules in the initial build prompt.

```text
Build a production-style MCP server using the Nubra API docs at https://nubra.io/products/api/docs/index.html as the source of truth.

Rules:
1. Prefer official Nubra docs behavior over assumptions.
2. Reuse shared client/service layers instead of duplicating request logic.
3. Separate:
   - low-level API client
   - service layer
   - MCP tool registration
4. Return structured JSON from every tool with:
   - ok
   - tool
   - data or error
5. Convert paise-like price fields into rupees for MCP responses where appropriate.
6. Preserve raw important fields like ref_id, exchange, lot_size, tick_size, expiry.
7. Use the public Index Master CSV for index-discovery questions.
8. Use the instruments master for exchange-specific tradable instrument lookups.
9. For margin, always treat total_margin as the authoritative required margin.
10. For “yesterday change”, use current price vs previous close from Nubra current-price API.
11. Add clear tool docstrings so an LLM can route user prompts correctly.
12. Cache instrument master and index master responses to reduce repeated calls.
13. Support NSE by default, but allow exchange override like BSE.
14. When matching indices, implement fuzzy normalization logic:
    - ignore spaces
    - ignore underscores
    - ignore punctuation
    - compare compact normalized tokens
15. When a user asks for an index name, closest index, or index instrument name:
    - query Index Master first
    - then compare closest aliases against instrument master
    - return both best index match and related instrument candidates
16. Use FastMCP tool registration and keep transport usable for stdio and streamable-http.
17. Build this in Python, not TypeScript.
18. Use FastAPI + uvicorn for the HTTP transport wrapper.
19. Use `requests` for Nubra REST calls.
20. Use `pydantic` models for request/response validation helpers.
```

---

## Build Prerequisites Prompt

Use this before asking the agent to write code if you want the output to closely match this implementation.

```text
Build this MCP in Python 3.11+.

Use:
- FastMCP for MCP tool registration
- FastAPI and uvicorn for HTTP serving
- requests for Nubra REST API calls
- pydantic for typed request/response models
- pyotp for TOTP login support

Project structure should be:
- server.py
- config.py
- nubra_client.py
- tools/auth.py
- tools/quotes.py
- tools/options.py
- tools/analytics.py
- tools/orders.py
- requirements.txt

Support:
- stdio transport
- streamable-http transport
- optional sse transport

Persist auth/session state locally so the server can reuse login state across restarts.
```

---

## Starter Prompt

Use this first.

```text
Build a Nubra MCP server in Python using https://nubra.io/products/api/docs/index.html as the primary source.

Technical requirements:
- Python 3.11+
- FastMCP
- FastAPI
- uvicorn
- requests
- pydantic
- pyotp

Start with:
- auth_status
- set_environment
- send_otp
- verify_otp
- verify_mpin
- login_auto
- logout

Then add:
- get_quote
- get_historical_data
- get_option_chain
- calculate_option_greeks
- find_delta_neutral_pairs
- analyze_option_greek_changes
- get_positions
- get_orders
- place_order
- square_off_position
- cancel_order
- get_strategy_pnl

Architecture requirements:
- `server.py` for FastMCP registration
- `nubra_client.py` for REST client and service layer
- `tools/*.py` for tool groups
- strong error handling
- structured responses
- reusable symbol resolution from instrument master
```

---

## Prompt To Add Current Price And Yesterday Change

```text
Extend the Nubra MCP with support for the Current Price API and user questions like:
- how much did NIFTY change over yesterday
- how much did RELIANCE change from previous close
- what is today’s move in BANKNIFTY

Use the Nubra docs for:
- Current Price
- Get Instruments

Requirements:
- add a client method for current price
- add `get_current_price(symbol, exchange="NSE")`
- add `get_yesterday_change(symbol, exchange="NSE")`
- for stocks/options, attach instrument metadata when resolvable
- for indices, work even if they are not in the tradable instruments master
- return:
  - symbol
  - exchange
  - current_price
  - previous_close
  - percent_change
  - absolute_change
  - direction
```

---

## Prompt To Add Margin

```text
Extend the Nubra MCP using the Get Margin docs.

Requirements:
- add a generic `get_margin(...)` tool wrapping Nubra margin_required
- use `total_margin` as the authoritative value
- support:
  - with_portfolio
  - with_legs
  - is_basket
  - basket_params

Then add a higher-level strategy tool:
- `get_atm_straddle_margins(symbols, exchange="NSE", expiry=None, lots=1, order_side="ORDER_SIDE_SELL")`

Behavior:
- resolve nearest-expiry ATM CE and PE for each underlying
- get lot size from instrument metadata
- basket the two legs
- call margin_required in basket mode
- return one row per underlying with:
  - underlying
  - expiry
  - atm_strike
  - call_leg
  - put_leg
  - lot_size
  - per_leg_order_qty
  - total_margin
  - margin_benefit
  - message

This should work for prompts like:
- how much margin is required for a straddle in NIFTY
- compare straddle margin for BANKNIFTY, RELIANCE, INFY, TCS
```

---

## Prompt To Add Instrument Master Lookups

```text
Extend the Nubra MCP using the Get Instruments docs.

Add:
- `get_instrument_details(symbol, exchange="NSE")`
- `find_instruments(exchange="NSE", symbol=None, asset=None, derivative_type=None, option_type=None, limit=10)`

The MCP should answer questions like:
- get me the ref id for HDFCBANK in BSE
- get me tick size of ICICIBANK from NSE
- get me lot size of RELIANCE
- show me matching option instruments for NIFTY

Return important fields such as:
- ref_id
- stock_name
- asset
- exchange
- derivative_type
- asset_type
- lot_size
- tick_size
- expiry
- option_type
- nubra_name or zanskar_name
```

---

## Prompt To Add Index Master Lookups

```text
Extend the Nubra MCP to support index-discovery questions using Nubra’s public Index Master CSV.

Use the flow:
1. fetch public index master from https://api.nubra.io/public/indexes?format=csv
2. search the closest rows for the user query
3. compare the closest aliases against the exchange instrument master
4. return both:
   - best matching index rows
   - related instrument candidates

Add:
- `find_index_details(query, exchange="NSE", limit=10, instrument_limit=10)`

Matching rules:
- normalize spaces, underscores, punctuation
- compare compact normalized forms
- compare token overlap
- support fuzzy aliases like:
  - INDIA VIX
  - INDIAVIX
  - INDIA_VIX
  - VIX
  - IndiaVIX

This should handle prompts like:
- get me instrument name of India VIX
- what is the Nubra index name for VIX
- show me closest index match for indiavix
```

---

## Prompt To Improve LLM Routing

```text
Update all MCP tool docstrings so an LLM can route user intent correctly.

Examples:
- `get_yesterday_change`: use when the user asks how much a stock, index, or option changed over yesterday or from previous close
- `get_instrument_details`: use when the user asks for ref_id, tick size, lot size, or nubra name
- `find_index_details`: use when the user asks for index names like India VIX, NIFTY, BANKNIFTY, FINNIFTY, or closest index match
- `get_atm_straddle_margins`: use when the user asks margin for a straddle in NIFTY, BANKNIFTY, RELIANCE, INFY, or TCS
```

---

## Webinar Demo Prompt Sequence

These are the prompts you can use live after the MCP is built.

### Auth

```text
Check Nubra authentication status.
```

```text
Set Nubra environment to PROD.
```

```text
Login to Nubra using auto-login.
```

### Quote And Price

```text
Get the latest quote for RELIANCE.
```

```text
Get current price of NIFTY.
```

```text
How much did BANKNIFTY change over yesterday?
```

```text
How much did RELIANCE change from previous close?
```

### Historical And Options

```text
Get 1-minute historical data for NIFTY for yesterday’s session.
```

```text
Get the option chain for NIFTY.
```

```text
Calculate option Greeks for NIFTY.
```

```text
Find delta-neutral option pairs for NIFTY.
```

```text
Analyze ATM option vega change for NIFTY and BANKNIFTY over today intraday.
```

### Margin

```text
How much margin is required for a straddle in NIFTY?
```

```text
Compare ATM straddle margin for NIFTY, BANKNIFTY, RELIANCE, INFY, and TCS.
```

### Instrument Master

```text
Get me the ref id for HDFCBANK in BSE.
```

```text
Get me the tick size of ICICIBANK from NSE.
```

```text
Get instrument details for TCS on NSE.
```

```text
Find matching option instruments for NIFTY on NSE.
```

### Index Master

```text
Get me the instrument name of India VIX.
```

```text
Find the closest Nubra index match for INDIAVIX.
```

```text
Show me related instrument candidates for VIX on NSE.
```

### Orders And Portfolio

```text
Show my positions.
```

```text
Show executed orders for today.
```

```text
Group executed orders by tag and show strategy PnL.
```

---

## Suggested “Master Prompt” For A Coding Agent

If you want one larger prompt instead of step-by-step prompts, use this.

```text
Build a production-style Nubra MCP server in Python using https://nubra.io/products/api/docs/index.html as the source of truth.

Technical stack:
- Python 3.11+
- FastMCP
- FastAPI
- uvicorn
- requests
- pydantic
- pyotp

The MCP should include:
- authentication
- environment switching
- quote lookup
- current price lookup
- yesterday change lookup
- historical data
- option chain
- option Greeks
- delta-neutral pair search
- option Greek-change analysis
- orders
- positions
- strategy pnl summary
- margin estimation
- ATM straddle margin estimation
- instrument master lookup
- public index master lookup
- fuzzy index matching

Implementation rules:
- use FastMCP
- use Python, not TypeScript
- use FastAPI + uvicorn for HTTP serving
- use requests for REST calls
- use pydantic models where helpful
- organize by client/service/tool layers
- return structured JSON
- convert paise-like fields to rupees where appropriate
- use instruments master for tradable instrument metadata
- use public index master CSV for index metadata
- for index questions, query index master first and then compare aliases against instrument master
- support exchange overrides like NSE and BSE
- use total_margin as the final required margin
- support prompts like:
  - how much did NIFTY change over yesterday
  - what is the ref id of HDFCBANK in BSE
  - what is the tick size of ICICIBANK in NSE
  - get me instrument name of India VIX
  - compare straddle margin for NIFTY, BANKNIFTY, RELIANCE, INFY, TCS
```

---

## Closing Notes For The Webinar

- Show the Nubra docs first, then show how each MCP tool maps to a doc page.
- Show the technical stack before code generation:
  - Python
  - FastMCP
  - FastAPI
  - uvicorn
  - requests
  - pydantic
  - pyotp
- Emphasize that the agent quality comes from:
  - correct tool boundaries
  - strong docstrings
  - structured responses
  - good symbol/index resolution
- For index handling, explain the difference between:
  - tradable instruments master
  - public index master
- For strategy margin, explain why `total_margin` is the only value that matters for actual required margin.
