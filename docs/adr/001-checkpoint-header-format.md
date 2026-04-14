# ADR 001: Checkpoint Header Format

## Status

Accepted

## Date

2026-04-06

## Context

Gravtory checkpoints step outputs to the database so workflows can resume after crashes. The checkpoint pipeline is: serialize → compress → encrypt → store. On restore, the inverse pipeline runs: decrypt → decompress → deserialize.

The challenge is that a checkpoint stored with one configuration (e.g., JSON + gzip + AES) must be restorable even if the Gravtory instance configuration changes. We need the checkpoint itself to be self-describing.

## Decision

We prepend a **1-byte header** to every checkpoint payload that encodes the pipeline configuration:

```
Bit 0:   compression enabled (0/1)
Bit 1:   encryption enabled (0/1)
Bits 2-4: serializer type (0=json, 1=msgpack, 2=pickle)
Bits 5-7: compressor type (0=none, 1=gzip, 2=lz4, 3=zstd)
```

This means `restore()` is fully self-describing — it reads the header byte, instantiates the correct deserializer/decompressor/decryptor, and reconstructs the Python object without needing external configuration.

## Consequences

- **Positive**: Checkpoints are portable across configuration changes. A checkpoint written with gzip can be read after switching to lz4.
- **Positive**: Only 1 byte of overhead per checkpoint.
- **Positive**: Enables key rotation — old data decrypted with old key, new data encrypted with new key.
- **Negative**: Maximum of 8 serializer types and 8 compressor types (3 bits each). Sufficient for foreseeable needs.
- **Negative**: Header format is a breaking change if modified. We version the encryption format separately (version byte in encrypted payload).

## Alternatives Considered

1. **Store config in a separate metadata table**: More flexible but adds a DB query per checkpoint restore. Rejected for performance.
2. **Use a JSON envelope**: Self-describing but wastes bytes and prevents binary-only payloads. Rejected.
3. **No header — require matching config**: Simplest but breaks on config changes. Rejected for robustness.
