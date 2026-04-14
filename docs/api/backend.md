# Backend API Reference

The abstract `Backend` class defines every storage operation that Gravtory
needs. Each concrete backend (PostgreSQL, SQLite, MySQL, MongoDB, Redis)
implements all abstract methods using database-native primitives.

To write a custom backend, subclass `Backend` and implement every abstract
method. Use `InMemoryBackend` as a reference implementation.

## Abstract Base Class

::: gravtory.backends.base
    options:
      show_root_heading: false
      members_order: source

## In-Memory Backend (Testing)

::: gravtory.backends.memory.InMemoryBackend
    options:
      show_root_heading: true
      members: false
