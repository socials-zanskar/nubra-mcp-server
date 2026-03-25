from __future__ import annotations

from typing import Any

from nubra_client import NubraAPIError, NubraService


def _success(tool: str, data: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "tool": tool, "data": data}


def _failure(tool: str, exc: Exception) -> dict[str, Any]:
    if isinstance(exc, NubraAPIError):
        return {
            "ok": False,
            "tool": tool,
            "error": {
                "message": str(exc),
                "status_code": exc.status_code,
                "details": exc.details,
            },
        }
    return {"ok": False, "tool": tool, "error": {"message": str(exc)}}


def register(mcp: Any, service: NubraService) -> None:
    @mcp.tool()
    def connect_nubra_mcp(phone: str | None = None, mpin: str | None = None, environment: str = "PROD") -> dict[str, Any]:
        """Recommended first-step onboarding flow. If phone or MPIN is missing, return the next-step prompt. If provided, switch to the selected environment, store the MPIN for the current session, and send an OTP."""
        try:
            return _success("connect_nubra_mcp", service.connect_nubra_mcp(phone=phone, mpin=mpin, environment=environment))
        except Exception as exc:
            return _failure("connect_nubra_mcp", exc)

    @mcp.tool()
    def auth_status() -> dict[str, Any]:
        """Check login state and return the standard nubra-mcp intro plus environment guidance. For market data, PROD is recommended. For order placement testing, use UAT."""
        try:
            return _success("auth_status", service.auth_status())
        except Exception as exc:
            return _failure("auth_status", exc)


    @mcp.tool()
    def set_environment(environment: str) -> dict[str, Any]:
        """Set the Nubra environment to UAT or PROD and clear the active session for the previous environment."""
        try:
            return _success("set_environment", service.set_environment(environment))
        except Exception as exc:
            return _failure("set_environment", exc)


    @mcp.tool()
    def switch_environment_and_send_otp(environment: str, phone: str | None = None) -> dict[str, Any]:
        """Recommended environment-switch entrypoint. Switch the Nubra environment, immediately clear the current session, and send an OTP for the target environment."""
        try:
            return _success(
                "switch_environment_and_send_otp",
                service.switch_environment_and_send_otp(environment=environment, phone=phone),
            )
        except Exception as exc:
            return _failure("switch_environment_and_send_otp", exc)


    @mcp.tool()
    def send_otp(phone: str | None = None, environment: str | None = None) -> dict[str, Any]:
        """Send an OTP for the selected environment. After this, ask for the OTP and call verify_otp or verify_otp_with_saved_mpin."""
        try:
            return _success("send_otp", service.send_otp(phone=phone, environment=environment))
        except Exception as exc:
            return _failure("send_otp", exc)


    @mcp.tool()
    def begin_auth_flow(phone: str, environment: str | None = None) -> dict[str, Any]:
        """Start the standard authentication flow for the selected environment. Then ask for OTP, call verify_otp, ask for MPIN, and call verify_mpin."""
        try:
            return _success("begin_auth_flow", service.begin_auth_flow(phone=phone, environment=environment))
        except Exception as exc:
            return _failure("begin_auth_flow", exc)


    @mcp.tool()
    def verify_otp(otp: str, phone: str | None = None) -> dict[str, Any]:
        """Verify the OTP for the current environment. If successful, ask for MPIN and call verify_mpin next."""
        try:
            return _success("verify_otp", service.verify_otp(otp=otp, phone=phone))
        except Exception as exc:
            return _failure("verify_otp", exc)


    @mcp.tool()
    def verify_mpin(mpin: str) -> dict[str, Any]:
        """Final login step. After the user provides MPIN, call this to establish the session, then resume the original task."""
        try:
            return _success("verify_mpin", service.verify_mpin(mpin))
        except Exception as exc:
            return _failure("verify_mpin", exc)

    @mcp.tool()
    def verify_otp_with_saved_mpin(otp: str, phone: str | None = None) -> dict[str, Any]:
        """Recommended fast login completion when MPIN is already configured. Verify OTP and then reuse the configured MPIN for the current environment."""
        try:
            return _success("verify_otp_with_saved_mpin", service.verify_otp_with_saved_mpin(otp=otp, phone=phone))
        except Exception as exc:
            return _failure("verify_otp_with_saved_mpin", exc)

    @mcp.tool()
    def complete_connect_with_otp(otp: str, phone: str | None = None) -> dict[str, Any]:
        """Second step of the guided connect flow. Verify OTP, reuse the MPIN that was provided earlier, and confirm the logged-in environment."""
        try:
            return _success("complete_connect_with_otp", service.complete_connect_with_otp(otp=otp, phone=phone))
        except Exception as exc:
            return _failure("complete_connect_with_otp", exc)

    @mcp.tool()
    def logout() -> dict[str, Any]:
        """Clear the active Nubra authentication session from the MCP server."""
        try:
            return _success("logout", service.logout())
        except Exception as exc:
            return _failure("logout", exc)
