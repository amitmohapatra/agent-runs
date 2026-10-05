"""Where a webhook may be delivered: the guard against a subscription that points the service
at its own network (SSRF).

A host is a public target when every address it resolves to is a global unicast address:
not private, loopback, link-local (cloud metadata services live there), carrier-grade NAT,
reserved or multicast; an IPv4 address mapped into IPv6 is judged as itself. A
subscription's URL is checked when it is created, and again for every delivery: its name may
resolve elsewhere by then. A delivery resolves the name once, checks every address, and
connects only to an address it checked (``pinned``), still naming the host in ``Host``, in
TLS SNI and in the certificate check, so a name that changes between the check and the
connection (DNS rebinding) cannot send it anywhere else. A deployment that delivers inside
its own network allows private targets (``RUNS__WEBHOOKS__ALLOW_PRIVATE_TARGETS``; ``dev``
does by default), and then nothing is resolved here.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx

from agent_runs.domain.errors import Unprocessable


class NotPublic(Exception):
    """The URL's host resolves to ``address``, which is not a public target."""

    def __init__(self, host: str, address: str) -> None:
        super().__init__(f"{host} resolves to {address}, which is not a public address")
        self.address = address


@dataclass(frozen=True)
class Target:
    """Where one attempt connects, and what its request carries to name the host there."""

    url: httpx.URL
    headers: dict[str, str] = field(default_factory=dict)
    extensions: dict[str, Any] = field(default_factory=dict)


async def public_addresses(url: str) -> list[str]:
    """Every address the URL's host resolves to, resolved once, in the resolver's order, when
    all are public targets. Raises ``NotPublic`` naming the first that is not, and
    ``OSError`` when the host does not resolve."""
    host = urlparse(url).hostname or ""
    resolved = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    addresses: list[str] = []
    for *_, sockaddr in resolved:
        address = ipaddress.ip_address(sockaddr[0])
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        if not address.is_global or address.is_multicast:
            raise NotPublic(host, str(address))
        if str(address) not in addresses:
            addresses.append(str(address))
    return addresses


def pinned(url: str, address: str) -> Target:
    """``url`` sent to ``address`` (one that was checked) and nowhere else: the connection
    goes to the address, while ``Host``, TLS SNI and the certificate's hostname check name
    the URL's host."""
    named = httpx.URL(url)
    return Target(
        url=named.copy_with(host=address),
        headers={"Host": named.netloc.decode("ascii")},
        extensions={"sni_hostname": named.host},
    )


async def require_public(url: str) -> None:
    """Refuse (422) a URL whose host does not resolve, or resolves to an address that is not
    a public target."""
    host = urlparse(url).hostname
    try:
        await public_addresses(url)
    except OSError as exc:
        raise Unprocessable(f"url's host {host} does not resolve") from exc
    except NotPublic as exc:
        raise Unprocessable(
            f"url's host {exc}: this deployment delivers only to public hosts"
        ) from exc
