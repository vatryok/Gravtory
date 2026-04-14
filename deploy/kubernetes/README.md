# Kubernetes Deployment

Deploy Gravtory on Kubernetes with auto-scaling workers and a dashboard.

## Prerequisites

- Kubernetes 1.27+
- kubectl configured
- PostgreSQL accessible from the cluster
- Container image built and pushed to a registry

## Quick Start

```bash
# 1. Create namespace
kubectl apply -f namespace.yaml

# 2. Configure secrets (edit values first!)
kubectl apply -f secret.yaml

# 3. Apply config
kubectl apply -f configmap.yaml

# 4. Deploy engine workers + dashboard
kubectl apply -f deployment.yaml

# 5. Expose services
kubectl apply -f service.yaml

# 6. Enable auto-scaling
kubectl apply -f hpa.yaml
```

## Architecture

```
┌─────────────────────────────────────────────┐
│                  Kubernetes                  │
│                                             │
│  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Dashboard   │  │   Engine Workers    │  │
│  │  (1 replica) │  │   (2-10 replicas)   │  │
│  │  port: 7777  │  │   HPA auto-scale    │  │
│  └──────┬───────┘  └──────────┬──────────┘  │
│         │                     │              │
│         └──────────┬──────────┘              │
│                    │                         │
│         ┌──────────▼──────────┐              │
│         │    PostgreSQL       │              │
│         │    (external)       │              │
│         └─────────────────────┘              │
└─────────────────────────────────────────────┘
```

## Configuration

Edit `configmap.yaml` and `secret.yaml` before deploying:

| Variable | Description |
|----------|-------------|
| `GRAVTORY_BACKEND` | PostgreSQL connection string |
| `GRAVTORY_WORKERS` | Worker concurrency per pod |
| `POSTGRES_PASSWORD` | Database password (secret) |
| `GRAVTORY_ENCRYPTION_KEY` | Checkpoint encryption key (secret) |
| `GRAVTORY_DASHBOARD_TOKEN` | Dashboard auth token (secret) |

## Scaling

The HPA scales engine workers based on CPU (70%) and memory (80%):
- **Min replicas**: 2
- **Max replicas**: 10
- **Scale up**: +2 pods per minute
- **Scale down**: -1 pod per 2 minutes (conservative)

## Monitoring

Prometheus metrics are exposed on port 9090 via `gravtory-metrics-service`.
Add a ServiceMonitor for Prometheus Operator integration.
