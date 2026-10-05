"""Where a webhook may be delivered: the guard against a subscription that points the service
at its own network (SSRF).

A host is a public target when every address it resolves to is a global unicast address:
not private, loopback, link-local (cloud metadata services live there), carrier-grade NAT,
reserved or multicast; an IPv4 address mapped into IPv6 is judged as itself. A
subscription's URL is checked when it is created, and again before every delivery, because
its name may resolve elsewhere by then (DNS rebinding). A deployment that delivers inside its
own network allows private targets (``RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS``; ``dev`` does by
default), and then nothing is resolved here.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlparse

from agent_runs.domain.errors import Unprocessable


async def private_address(url: str) -> str | None:
    """The first address the URL's host resolves to that is not a public target, or ``None``
    when every one is. Raises ``OSError`` when the host does not resolve."""
    host = urlparse(url).hostname or ""
    resolved = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    for *_, sockaddr in resolved:
        address = ipaddress.ip_address(sockaddr[0])
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        if not address.is_global or address.is_multicast:
            return str(address)
    return None


async def require_public(url: str) -> None:
    """Refuse (422) a URL whose host does not resolve, or resolves to an address that is not
    a public target."""
    host = urlparse(url).hostname
    try:
        address = await private_address(url)
    except OSError as exc:
        raise Unprocessable(f"url's host {host} does not resolve") from exc
    if address is not None:
        raise Unprocessable(
            f"url's host {host} resolves to {address}, which is not a public address: "
            "this deployment delivers only to public hosts"
        )
