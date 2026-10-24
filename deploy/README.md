# deploy/

Ready-to-use ways to run one `rt` node in production. Pick one; all of them use the same image or binary and the
same rules: TLS in a reverse proxy in front of the public listener, control API and metrics never exposed, secrets in
the environment or in files, never in `rt.toml`.

| Path | What |
|---|---|
| [`rt.toml`](rt.toml) | production defaults (loopback binds, `/var/lib/rt`, proxy on localhost trusted). Packages install it as `/etc/rt/rt.toml`; the image ships it as `/etc/rt/defconfigs/production.toml` |
| [`compose/`](compose/) | Docker Compose: `ghcr.io/21-ci/rt` behind Caddy (automatic HTTPS), read-only root filesystem, no capabilities |
| [`kubernetes/rt.yaml`](kubernetes/rt.yaml) | plain manifests: StatefulSet + PVC, probes, Services, NetworkPolicy, secrets as files |
| systemd | the unit and its hardening live with the packages: [`packaging/systemd/rt.service`](../packaging/systemd/rt.service) (installed by the deb/rpm packages) |
| Homebrew (macOS) | `brew install 21-ci/tap/rt && brew services start rt` ([`packaging/homebrew/`](../packaging/homebrew/)) |

The repository root `docker-compose.yml` is the **development** stack (builds the image from source, everything on
loopback); use `deploy/compose/` for a server.

## Compose

```bash
cd deploy/compose
cp ../../.env.example .env
sed -i.bak "s/^RT_ROOT_TOKEN=.*/RT_ROOT_TOKEN=$(openssl rand -hex 32)/; s/^RT_MASTER_KEY=.*/RT_MASTER_KEY=$(openssl rand -hex 32)/" .env && rm .env.bak
printf 'RT_DOMAIN=fn.example.com\nACME_EMAIL=ops@example.com\n' >> .env    # DNS A/AAAA record must point here
docker compose up -d
curl -fsS http://127.0.0.1:9090/readyz && curl -fsS https://fn.example.com/s/<slug>/
```

Pin `RT_IMAGE=ghcr.io/21-ci/rt:<version>` in `.env` and verify the image before you roll it out (see
[SECURITY.md](../SECURITY.md#verifying-releases)). Back up the `rt-data` volume **and** the master key.

## Kubernetes

See the header of [`kubernetes/rt.yaml`](kubernetes/rt.yaml). `rt` is single-node in v1: one replica, one volume.
Point your Ingress or Gateway at the `rt-public` Service and set `ingress.trusted_proxies` in the ConfigMap to the
controller's pod CIDR.

## systemd (deb / rpm)

```bash
sudo apt install rt            # or: sudo dnf install rt   (repositories: packaging/README.md)
sudoedit /etc/default/rt       # RT_ROOT_TOKEN, RT_MASTER_KEY  (rpm: /etc/sysconfig/rt)
sudo systemctl enable --now rt
journalctl -u rt -f
```
