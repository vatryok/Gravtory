# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Framework integrations — Django, FastAPI, Flask.

Available integrations:

- ``gravtory.contrib.fastapi`` — FastAPI dependency injection and lifespan hooks

Planned integrations:

- ``gravtory.contrib.django`` — Django management commands and app config
- ``gravtory.contrib.flask`` — Flask extension with app factory support

For Django and Flask, integrate Gravtory manually using the ``async with``
context manager or the ``start()``/``shutdown()`` lifecycle methods.
See the README for examples.
"""
