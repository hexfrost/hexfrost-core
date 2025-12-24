from __future__ import annotations

from enum import Enum


class ResponseMessages(str, Enum):
    header_missing = "Authorization header missing"
    invalid_scheme = "Invalid authentication scheme"
    invalid_token = "Invalid token or insufficient permissions"
    internal_server_error = "Internal Server Error"
