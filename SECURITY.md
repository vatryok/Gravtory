# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 1.0.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Gravtory, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please email: **vatryok@protonmail.com**

Include:
- Description of the vulnerability
- Steps to reproduce
- Impact assessment
- Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide a timeline for a fix within 7 days.

## Security Model

### Data at Rest
- **Encryption**: AES-256-GCM authenticated encryption for checkpoint data (opt-in via `gravtory[encryption]`)
- **Key derivation**: PBKDF2-HMAC-SHA256 with 600,000 iterations
- **Key rotation**: Supported via the KeyManager enterprise module
- **Default serializer**: JSON (safe). Pickle requires explicit opt-in with `RestrictedUnpickler`

### Database Security
- **Parameterized queries**: All SQL uses parameterized statements for user-supplied values
- **Connection strings**: Read from environment variables (`GRAVTORY_BACKEND`), never hardcoded
- **Minimal privileges**: Gravtory only needs SELECT, INSERT, UPDATE, DELETE on its own tables

### Dashboard Security
- **Authentication**: Token-based auth via `Authorization: Bearer <token>` header for the dashboard API
- **CSRF**: State-mutating endpoints (POST/DELETE) are protected by Bearer token authentication, not cookies. Because the dashboard does not use cookie-based sessions, CSRF attacks via form submissions are not applicable. If cookie-based auth is added in the future, CSRF tokens must be implemented.
- **XSS prevention**: All user data escaped in templates
- **CORS**: Configurable origin allowlist

### Pickle Security
Gravtory's `PickleSerializer` uses a `RestrictedUnpickler` that only allows a configurable allowlist of classes. **Pickle is NOT the default serializer** — JSON is. Using pickle requires explicit opt-in:

```python
from gravtory.serialization.pickle import PickleSerializer
grav = Gravtory(backend="...", serializer=PickleSerializer(allowed_classes=[...]))
```

### Dependencies
- Automated vulnerability scanning via `pip-audit` in CI
- Dependabot configured for weekly dependency updates
- Minimal required dependencies; optional extras for specific backends

## Security-Relevant Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `serializer` | `JSONSerializer` | Safe by default. Pickle opt-in only |
| `encryption_key` | `None` | Set via env var for checkpoint encryption |
| `auth_token` | Auto-generated | Required for dashboard; auto-generated if not set |
| `allowed_pickle_classes` | Required | Allowlist for RestrictedUnpickler; `ConfigurationError` raised if omitted |
