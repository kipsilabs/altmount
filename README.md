# AltMount

<p align="center">
  <img src="./docs/static/img/logo.png" alt="AltMount Logo" width="150" height="150" />
</p>

A WebDAV server backed by NZB/Usenet that provides seamless access to Usenet content through standard WebDAV protocols.

[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/qbt52hh7sjd)

## 📖 Documentation

**[View Full Documentation →](https://javi11.github.io/altmount/)**

Complete setup guides, configuration options, API reference, and troubleshooting information.

For legacy metadata installations, see the [v0.2.1 pre-V3 maintenance release and upgrade notes](docs/pre-v3-maintenance.md). Pin that version to receive the security backports without adopting the V3 metadata migration.

## Quick Start

### Docker (Recommended)

```bash
services:
  altmount:
    extra_hosts:
      - "host.docker.internal:host-gateway" # Optional if you rclone is outside the container
    image: ghcr.io/javi11/altmount:latest
    container_name: altmount
    environment:
      - PUID=1000
      - PGID=1000
      - PORT=8080
      - JWT_SECRET=change-me-to-a-strong-random-secret # Required when login is enabled (auth.login_required: true)
      - COOKIE_DOMAIN=localhost # Must match the domain/IP where web interface is accessed
    volumes:
      - ./config:/config
      - /mnt:/mnt:rshared
      - /metadata:/metadata # This is optional you can still use /mnt
      - /var/run/docker.sock:/var/run/docker.sock # Required for the auto-update feature
    group_add:
      - "999" # GID of the docker group on the host (run `getent group docker | cut -d: -f3` to find yours)
    ports:
      - "8080:8080"
    restart: unless-stopped
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
```

### CLI Installation

```bash
go install github.com/javi11/altmount@latest
altmount serve --config config.yaml
```

## Windows: Enable Long Path Support

The Windows AltMount binaries are built with a long-path-aware manifest, which
opts the process in to paths longer than the legacy `MAX_PATH` (260 character)
limit. However, Windows also requires the matching system-wide setting to be
enabled before long paths actually work — without it, you may see errors like
`The filename or extension is too long` when accessing deeply nested releases.

Enable it once per machine in an **elevated PowerShell** prompt (Run as
administrator), then restart AltMount:

```powershell
New-ItemProperty `
  -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" `
  -Name "LongPathsEnabled" `
  -Value 1 `
  -PropertyType DWORD `
  -Force
```

Equivalent via Group Policy: `Computer Configuration → Administrative Templates
→ System → Filesystem → Enable Win32 long paths`.

This setting requires Windows 10 version 1607 (build 14393) or newer. A reboot
is not strictly required, but any already-running process — including AltMount
and your file manager — needs to be restarted to pick up the change.

## Links

- 📚 [Documentation](https://altmount.kipsilabs.top)
- 🐛 [Issues](https://github.com/javi11/altmount/issues)
- 💬 [Discussions](https://github.com/javi11/altmount/discussions)

## Contributing

See the [Development Guide](https://altmount.kipsilabs.top/docs/Development/setup). Development/setup for information on setting up a development environment and contributing to the project.

## License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.
 
