"""Security utilities and SSRF protection for external notification delivery."""

from __future__ import annotations

import ipaddress
import socket
import urllib.parse

# Carrier-Grade NAT (RFC 6598) and Network Interconnect Benchmarking (RFC 2544)
_CGNAT_NET = ipaddress.ip_network("100.64.0.0/10")
_BENCHMARK_NET = ipaddress.ip_network("198.18.0.0/15")


class SsrfProtectionError(ValueError):
    """Raised when an external notification URL violates SSRF or security constraints."""


def is_forbidden_ip(ip: str | ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Check whether an IP address belongs to a private, loopback, or reserved range.

    Parameters
    ----------
    ip:
        IP address as a string or ipaddress object.

    Returns
    -------
    bool:
        True if the IP is forbidden (private, loopback, link-local, multicast,
        reserved, unspecified, CGNAT, benchmark); False otherwise.
    """
    if isinstance(ip, str):
        try:
            ip_obj: ipaddress.IPv4Address | ipaddress.IPv6Address = ipaddress.ip_address(
                ip.strip("[]")
            )
        except ValueError:
            return True
    else:
        ip_obj = ip

    # Unpack IPv4-mapped IPv6 addresses (e.g. ::ffff:127.0.0.1 -> 127.0.0.1)
    if isinstance(ip_obj, ipaddress.IPv6Address) and ip_obj.ipv4_mapped is not None:
        ip_obj = ip_obj.ipv4_mapped

    if (
        ip_obj.is_loopback
        or ip_obj.is_private
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
        or ip_obj.is_unspecified
    ):
        return True

    if isinstance(ip_obj, ipaddress.IPv4Address):
        if ip_obj in _CGNAT_NET or ip_obj in _BENCHMARK_NET:
            return True

    return False


def validate_webhook_url(
    url: str,
    allow_private_ips: bool = False,
    allow_insecure_http: bool = False,
    resolve_dns: bool = True,
) -> str:
    """Validate webhook destination URL against SSRF and protocol restrictions.

    Parameters
    ----------
    url:
        Webhook URL string.
    allow_private_ips:
        If True, allow private/loopback/link-local destination IPs (for testing only).
    allow_insecure_http:
        If True, allow unencrypted 'http://' scheme (for testing only).
    resolve_dns:
        If True, resolve hostname via DNS to verify target IP ranges.

    Returns
    -------
    str:
        The validated URL.

    Raises
    ------
    SsrfProtectionError:
        If the scheme, host, or resolved IP violates security constraints.
    """
    if not url or not isinstance(url, str):
        raise SsrfProtectionError("Webhook URL must be a non-empty string.")

    parsed = urllib.parse.urlsplit(url.strip())

    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        raise SsrfProtectionError(
            f"Unsupported URL scheme '{scheme}'. Only HTTPS is permitted in production."
        )
    if scheme == "http" and not allow_insecure_http:
        raise SsrfProtectionError(
            "Insecure HTTP scheme is not allowed in production. Use HTTPS."
        )

    if parsed.username or parsed.password:
        raise SsrfProtectionError(
            "URL credentials (username/password) are not permitted in webhook URLs."
        )

    hostname = (parsed.hostname or "").strip()
    if not hostname:
        raise SsrfProtectionError("Webhook URL must contain a valid hostname or IP address.")

    if not allow_private_ips:
        # Check if hostname is an IP literal
        is_ip_literal = False
        try:
            ip_obj = ipaddress.ip_address(hostname.strip("[]"))
            is_ip_literal = True
        except ValueError:
            is_ip_literal = False

        if is_ip_literal:
            if is_forbidden_ip(ip_obj):
                raise SsrfProtectionError(
                    f"Destination IP '{ip_obj}' is in a restricted private/local range."
                )
        elif resolve_dns:
            port = parsed.port or (443 if scheme == "https" else 80)
            try:
                addrinfo = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
            except socket.gaierror as exc:
                raise SsrfProtectionError(
                    f"Could not resolve webhook destination hostname '{hostname}': {exc}"
                ) from exc

            if not addrinfo:
                raise SsrfProtectionError(
                    f"No IP addresses resolved for hostname '{hostname}'."
                )

            for item in addrinfo:
                ip_str = item[4][0]
                try:
                    resolved_ip = ipaddress.ip_address(ip_str)
                except ValueError:
                    continue
                if is_forbidden_ip(resolved_ip):
                    raise SsrfProtectionError(
                        f"Destination hostname '{hostname}' resolves to restricted IP "
                        f"'{resolved_ip}'."
                    )

    return url.strip()
