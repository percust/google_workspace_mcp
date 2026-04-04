from types import SimpleNamespace

import core.server as server_module


def test_configure_server_for_http_uses_base_required_scopes(monkeypatch):
    captured = {}

    class FakeGoogleProvider:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(server_module, "get_transport_mode", lambda: "streamable-http")
    monkeypatch.setattr(server_module, "GoogleProvider", FakeGoogleProvider)
    monkeypatch.setattr(
        server_module,
        "get_current_scopes",
        lambda: [
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email",
            "openid",
        ],
    )
    monkeypatch.setattr(server_module, "set_auth_provider", lambda provider: None)

    monkeypatch.setattr(
        "auth.oauth_config.get_oauth_config",
        lambda: SimpleNamespace(
            is_oauth21_enabled=lambda: True,
            is_configured=lambda: True,
            is_external_oauth21_provider=lambda: False,
            client_id="client-id",
            client_secret="client-secret",
            get_oauth_base_url=lambda: "https://workspace-mcp.example.test",
            redirect_path="/oauth2callback",
        ),
    )

    server_module.configure_server_for_http()

    assert captured["required_scopes"] == sorted(server_module.BASE_SCOPES)
    assert captured["valid_scopes"] == sorted(server_module.get_current_scopes())
