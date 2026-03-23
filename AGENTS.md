# Nubra MCP Agent Instructions

This repo contains a read-only Nubra MCP server.

## Required Auth Behavior

Before using any protected Nubra tool:

1. Call `auth_status`.
2. If `requires_login` is `true`:
   ask the user for phone number
   call `begin_auth_flow`
   ask the user for the OTP
   call `verify_otp`
   ask the user for the MPIN
   call `verify_mpin`
3. After authentication succeeds, resume the original user request.

## Important Rules

- Do not assume a session is valid without checking `auth_status`.
- If a protected tool returns an authentication error, restart the login flow from phone number.
- Authentication in this MCP is OTP -> MPIN only.
- Trading is disabled. Do not attempt order placement, cancellation, square-off, or strategy execution.
- Prefer UAT for validation unless the user explicitly needs production data.

## Suggested Agent Pattern

When the user asks for market/account data:

1. Check `auth_status`.
2. If logged in, proceed normally.
3. If not logged in:
   ask for phone number
   call `begin_auth_flow`
   finish OTP and MPIN verification
   continue with the task the user originally asked for
