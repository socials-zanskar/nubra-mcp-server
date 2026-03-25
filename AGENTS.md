# Nubra MCP Agent Instructions

This repository contains the Nubra MCP server.

## Standard Connect Flow

When the user says `connect to nubra mcp`, use the following standardized flow:

1. Call `connect_nubra_mcp` first.
2. If the user did not yet provide environment, phone number, or MPIN:
   ask for:
   - environment
   - phone number
   - MPIN
3. Prefer:
   - `PROD` for market data
   - `UAT` for order placement testing
4. After the user provides environment, phone number, and MPIN:
   call `connect_nubra_mcp(phone, mpin, environment)`
5. After the OTP is sent:
   ask the user for the OTP
6. Call `complete_connect_with_otp`
7. After authentication succeeds:
   tell the user which environment is logged in
   then continue with the original request

Expected response pattern for the connect flow:

- first: `Connected successfully with nubra mcp.`
- next: ask for environment, phone number, and MPIN if not yet provided
- after OTP verification: confirm the logged-in environment

## Required Auth Behavior

Before using any protected Nubra tool:

1. Call `auth_status`.
2. If `requires_login` is `true`, prefer the standardized connect flow above.
3. If the user is already mid-login:
   - continue from the current step
   - do not restart unnecessarily
4. After authentication succeeds, resume the original user request.

## Important Rules

- Do not assume a session is valid without checking `auth_status`.
- If a protected tool returns an authentication error, restart the login flow from phone number.
- Authentication in this MCP is OTP -> MPIN only.
- If the user already provided MPIN during `connect_nubra_mcp`, reuse it through `complete_connect_with_otp`.
- For market data requests, recommend `PROD` because it provides the most complete data coverage.
- For order placement testing, recommend `UAT`.
- Do not use non-Nubra sources for market data.
- Display user-facing prices in rupees when the MCP already provides formatted rupee fields.

## Suggested Agent Pattern

When the user asks for market/account data:

1. Check `auth_status`.
2. If logged in, proceed normally.
3. If not logged in:
   use the standardized connect flow
4. If the user is in `UAT` and asks for market data:
   suggest `PROD`
5. If the user is in `PROD` and asks for order placement testing:
   suggest `UAT`
