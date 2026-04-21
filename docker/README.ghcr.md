# Using Pre-built Aesthetic Hub Images

This guide shows how to use the pre-built Docker images from GitHub Container Registry instead of building locally.

## 📦 **Pre-built Images**

The following images are automatically built and pushed on every commit to `main`:

| Image | Registry URL | Purpose |
|-------|-------------|---------|
| **immich-server** | `ghcr.io/<your-org>/immich-server:latest` | Immich backend with aesthetic scoring |
| **immich-machine-learning** | `ghcr.io/<your-org>/immich-machine-learning:latest` | CLIP embeddings service |
| **aesthetic-service** | `ghcr.io/<your-org>/aesthetic-service:latest` | Aesthetic scoring inference service |

## 🚀 **Quick Start**

### **1. Create `.env` file**

```bash
# Copy example env
cp example.ghcr.env .env

# Edit with your settings
nano .env
```

**Required: Set your GitHub organization**
```bash
# In .env file
GITHUB_ORG=your-github-org  # ← CHANGE THIS
IMAGE_TAG=latest
```

**Required: Change default passwords**
```bash
# In .env file
DB_PASSWORD=your-secure-password
MINIO_ROOT_PASSWORD=your-secure-password
MLFLOW_DB_PASSWORD=your-secure-password
```

**Optional: Customize paths**
```bash
# In .env file
UPLOAD_LOCATION=./uploads
DB_DATA_LOCATION=./postgres-data
```

### **2. Pull Images**

```bash
# Login to GitHub Container Registry (if images are private)
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin

# Pull images
docker compose -f docker-compose.ghcr.yml pull
```

### **3. Start Services**

```bash
docker compose -f docker-compose.ghcr.yml up -d
```

### **4. Verify**

```bash
# Check all services are running
docker compose -f docker-compose.ghcr.yml ps

# Check logs
docker compose -f docker-compose.ghcr.yml logs -f immich-server
docker compose -f docker-compose.ghcr.yml logs -f aesthetic-service

# Access services
# Immich UI: http://localhost:2283
# MinIO Console: http://localhost:9001
# MLflow: http://localhost:5001
# Adminer: http://localhost:8080
```

---

## 🏗️ **Architecture**

```
┌─────────────────────────────────────────────────────────────┐
│ Pre-built Images (from GHCR)                                 │
├─────────────────────────────────────────────────────────────┤
│ • immich-server:latest                                       │
│ • immich-machine-learning:latest                             │
│ • aesthetic-service:latest                                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Dependencies (User brings up)                                │
├─────────────────────────────────────────────────────────────┤
│ • Postgres (immich + mlflow)                                 │
│ • Redis                                                      │
│ • MinIO                                                      │
│ • MLflow                                                     │
│ • Adminer (optional)                                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔄 **Updating Images**

### **Pull Latest**
```bash
docker compose -f docker-compose.ghcr.yml pull
docker compose -f docker-compose.ghcr.yml up -d
```

### **Use Specific Version**
```bash
# Use specific commit SHA
IMAGE_TAG=abc123def docker compose -f docker-compose.ghcr.yml up -d

# Use main branch
IMAGE_TAG=main docker compose -f docker-compose.ghcr.yml up -d
```

---

## 🐛 **Troubleshooting**

### **Image Pull Fails**

If images are private, you need to authenticate:
```bash
# Create GitHub Personal Access Token with read:packages scope
# https://github.com/settings/tokens

# Login
echo $GITHUB_TOKEN | docker login ghcr.io -u YOUR_USERNAME --password-stdin
```

### **Service Won't Start**

Check logs:
```bash
docker compose -f docker-compose.ghcr.yml logs aesthetic-service
```

Common issues:
- **Postgres not ready**: Wait for healthcheck to pass
- **MinIO not initialized**: Check bucket-init logs
- **Wrong image tag**: Verify `GITHUB_ORG` and `IMAGE_TAG` in `.env`

### **Reset Everything**

```bash
# Stop and remove containers
docker compose -f docker-compose.ghcr.yml down

# Remove volumes (WARNING: deletes all data)
docker compose -f docker-compose.ghcr.yml down -v

# Start fresh
docker compose -f docker-compose.ghcr.yml up -d
```

---

## 📊 **Available Tags**

| Tag | Description | Use Case |
|-----|-------------|----------|
| `latest` | Latest stable build from main | Production |
| `main` | Latest commit on main branch | Testing latest features |
| `<commit-sha>` | Specific commit | Pinned version for reproducibility |

Example:
```bash
# Latest stable
GITHUB_ORG=myorg IMAGE_TAG=latest docker compose -f docker-compose.ghcr.yml up

# Specific commit
GITHUB_ORG=myorg IMAGE_TAG=abc123def456 docker compose -f docker-compose.ghcr.yml up
```

---

## 🔐 **Security Notes**

1. **Change default passwords** in `.env`:
   - `DB_PASSWORD`
   - `MINIO_ROOT_PASSWORD`
   - `MLFLOW_DB_PASSWORD`

2. **Use secrets** for production:
   ```bash
   # Don't commit .env to git
   echo ".env" >> .gitignore
   ```

3. **Private images**: If your images are private, users need:
   - GitHub account with access to your org
   - Personal Access Token with `read:packages` scope

---

## 📚 **Next Steps**

- [Run the batch pipeline](../aesthetic/pipelines/batch/README.md)
- [Configure training](../aesthetic/pipelines/batch/train_personalized.yaml)
- [Monitor with Grafana](../deploy/k8s/monitoring/README.md)

---

## 🆘 **Support**

- **Issues**: https://github.com/your-org/immich/issues
- **Docs**: https://github.com/your-org/immich/tree/main/aesthetic
- **Discord**: [Your Discord Link]
