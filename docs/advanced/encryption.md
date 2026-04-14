# Encryption

Gravtory supports encrypting checkpoint data at rest using AES-256-GCM authenticated encryption.

## Setup

```bash
pip install gravtory[encryption]
```

```python
from gravtory import Gravtory
from gravtory.serialization.encryption import AES256GCMEncryptor

encryptor = AES256GCMEncryptor(key="your-secret-key-from-env-var")

grav = Gravtory(
    "postgresql://localhost/mydb",
    encryptor=encryptor,
)
```

## How It Works

The encryption pipeline:

```
Step output → JSON serialize → Gzip compress (optional) → AES-256-GCM encrypt → Store in DB
```

### Key Derivation

- User-provided key string → PBKDF2-HMAC-SHA256 (600,000 iterations) → 256-bit AES key
- Each encryption uses a random 12-byte nonce
- Authentication tag prevents tampering

### Security Properties

- **Confidentiality** — checkpoint data encrypted with AES-256
- **Integrity** — GCM authentication tag detects any modification
- **Unique nonces** — random nonce per encryption prevents pattern analysis

## Key Management

### Environment Variables (Recommended)

```python
import os
encryptor = AES256GCMEncryptor(key=os.environ["GRAVTORY_ENCRYPTION_KEY"])
```

### Key Rotation

The enterprise `KeyManager` supports key rotation:

```python
from gravtory.enterprise.key_manager import KeyManager

km = KeyManager(
    current_key="new-key-2025",
    previous_keys=["old-key-2024"],
)

grav = Gravtory("postgresql://localhost/mydb", key_manager=km)
```

Old data is decrypted with previous keys and re-encrypted with the current key on access.

## Best Practices

- **Never hardcode keys** — use environment variables or a secrets manager
- **Rotate keys periodically** — use the KeyManager for zero-downtime rotation
- **Enable encryption for sensitive data** — PII, financial data, health records
- **Test encryption** — verify round-trip with `AES256GCMEncryptor.encrypt()` / `.decrypt()`
