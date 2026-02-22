# ExternalDNS - PS Cloud DNS Webhook

This is an [ExternalDNS](https://github.com/kubernetes-sigs/external-dns) webhook provider for [PS Cloud Services](https://www.ps.kz) DNS.

## Installation

Run this webhook as a sidecar container with the official ExternalDNS [Helm chart](https://artifacthub.io/packages/helm/external-dns/external-dns).

```yaml
registry: txt
txtOwnerId: <owner-id>

provider:
  name: webhook
  webhook:
    image:
      repository: ghcr.io/supervillain3000/external-dns-pscloud-webhook
      tag: v1.0.0
      pullPolicy: IfNotPresent
    env:
      - name: PS_DNS_TOKEN
        valueFrom:
          secretKeyRef:
            name: pscloud-webhook-secret
            key: PS_DNS_TOKEN
    args:
      - --webhook-addr=:8888
      - --health-addr=:8080
      - --graphql-endpoint=https://console.ps.kz/dns/graphql
      - --domain-filter=example.com
      - --log-level=info
    livenessProbe:
      httpGet:
        path: /livez
    readinessProbe:
      httpGet:
        path: /readyz
    service:
      port: 8888

extraArgs:
  webhook-provider-url: http://127.0.0.1:8888
```

```bash
helm upgrade --install external-dns external-dns/external-dns \
  -n external-dns --create-namespace \
  -f values.yaml
```

## Command line arguments

- `--webhook-addr` (default: `:8888`)
- `--health-addr` (default: `:8080`)
- `--graphql-endpoint` (default: `https://console.ps.kz/dns/graphql`)
- `--domain-filter` (default: empty, comma-separated domains)
- `--request-timeout` (default: `30s`)
- `--zones-page-size` (default: `100`)
- `--graphql-max-retries` (default: `3`)
- `--graphql-retry-initial-backoff` (default: `200ms`)
- `--graphql-retry-max-backoff` (default: `2s`)
- `--graphql-retry-jitter` (default: `100ms`)
- `--dry-run` (default: `false`)
- `--log-level` (default: `info`)

Health and metrics endpoints:

- `GET /livez`
- `GET /readyz`
- `GET /healthz`
- `GET /metrics`

## Authentication

Authentication header is `X-User-Token`.

Get IAM token:

- [PS Cloud Services IAM access settings](https://docs.ps.kz/ru/account/faq/general-console-services/iam-access-settings)

Create a Secret with key `PS_DNS_TOKEN` and value = IAM token:

```bash
kubectl -n external-dns create secret generic pscloud-webhook-secret \
  --from-literal=PS_DNS_TOKEN='<IAM_TOKEN>'
```
