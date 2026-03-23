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
    def auth_status() -> dict[str, Any]:
        """Check whether login is required before protected tools. Agents should call this first; if requires_login is true, ask for phone number, then OTP, then MPIN."""
        try:
            return _success("auth_status", service.auth_status())
        except Exception as exc:
            return _failure("auth_status", exc)


    @mcp.tool()
    def set_environment(environment: str) -> dict[str, Any]:
        """Set the Nubra environment to UAT or PROD and clear any active session for the previous environment."""
        try:
            return _success("set_environment", service.set_environment(environment))
        except Exception as exc:
            return _failure("set_environment", exc)


    @mcp.tool()
    def send_otp(phone: str | None = None, environment: str | None = None) -> dict[str, Any]:
        """First login step. Ask the user for phone number, then call this to send the OTP. After this, ask for the OTP and call verify_otp."""
        try:
            return _success("send_otp", service.send_otp(phone=phone, environment=environment))
        except Exception as exc:
            return _failure("send_otp", exc)


    @mcp.tool()
    def begin_auth_flow(phone: str, environment: str | None = None) -> dict[str, Any]:
        """Recommended auth entrypoint. After getting the user's phone number, call this first. Then ask for OTP, call verify_otp, ask for MPIN, and call verify_mpin."""
        try:
            return _success("begin_auth_flow", service.begin_auth_flow(phone=phone, environment=environment))
        except Exception as exc:
            return _failure("begin_auth_flow", exc)


    @mcp.tool()
    def verify_otp(otp: str, phone: str | None = None) -> dict[str, Any]:
        """Second login step. After the user provides the OTP, call this. If successful, ask for MPIN and call verify_mpin next."""
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
    def logout() -> dict[str, Any]:
        """Clear the active Nubra authentication session from the MCP server."""
        try:
            return _success("logout", service.logout())
        except Exception as exc:
            return _failure("logout", exc)
