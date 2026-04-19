# Aesthetic Hub to Immich Consolidation

## Overview

The `aesthetic_hub` folder has been consolidated into the `immich` repository structure. All deployment configurations, scoring service code, and documentation have been moved to appropriate locations within the immich directory.

## New Directory Structure

```
immich/
├── scoring-service/          # Python FastAPI service (formerly aesthetic_hub/scoring_service/)
│   ├── main.py
│   ├── db.py
│   ├── models.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── ...
├── deploy/
│   ├── k8s/                 # Kubernetes manifests (formerly aesthetic_hub/k8s/)
│   │   ├── app/
│   │   ├── autoscaling/
│   │   ├── canary/
│   │   ├── production/
│   │   ├── staging/
│   │   ├── monitoring/
│   │   ├── platform/
│   │   ├── serving/
│   │   └── workflows/
│   ├── ansible/             # Ansible playbooks (formerly aesthetic_hub/ansible/)
│   │   ├── argocd/
│   │   ├── general/
│   │   ├── k8s/
│   │   ├── monitoring/
│   │   ├── post_k8s/
│   │   ├── pre_k8s/
│   │   ├── ansible.cfg
│   │   └── inventory.yml
│   ├── terraform/           # Terraform configs (formerly aesthetic_hub/tf/)
│   │   ├── kvm/
│   │   └── ...
│   └── scripts/             # Database setup scripts (formerly aesthetic_hub/scripts/)
│       ├── create_immich_readonly_user.sql
│       ├── setup_immich_readonly.sh
│       └── README.md
├── docs/
│   └── aesthetic-scoring/   # Aesthetic scoring docs (formerly aesthetic_hub/docs/)
│       ├── architecture.md
│       ├── bringup.md
│       ├── infra-table.md
│       ├── logging-configuration.md
│       └── safeguarding.md
├── server/                  # NestJS backend (with aesthetic integration module)
│   └── src/
│       └── modules/
│           └── aesthetic-integration/
└── web/                     # SvelteKit frontend
```

## Updated Path References

The following files have been updated with new paths:

### Ansible Configuration
- **File**: `immich/deploy/ansible/ansible.cfg`
- **Change**: `inventory = /work/immich/deploy/ansible/inventory.yml`

### Monitoring Deployment
- **File**: `immich/deploy/ansible/monitoring/deploy_monitoring.yml`
- **Change**: `repo_root: /home/cc/immich`
- **Change**: `monitoring_dir: "{{ repo_root }}/deploy/k8s/monitoring"`

### Database Setup Scripts
- **File**: `immich/deploy/scripts/README.md`
- **Change**: Updated kubectl cp commands to reference `immich/deploy/scripts/`

## What Stayed the Same

The following references were NOT changed (intentionally):

1. **Database names**: `aesthetic_hub` database name remains unchanged
2. **GitHub repository URLs**: References to `github.com/22navyakumar/aesthetic_hub` remain unchanged
3. **Docker image names**: `ghcr.io/22navyakumar/aesthetic_hub/*` image references remain unchanged
4. **Kubernetes namespaces**: `aesthetic-hub` namespace remains unchanged

## Deployment Instructions

### Building the Scoring Service

```bash
cd immich/scoring-service
docker build -t your-registry/aesthetic-scoring-service:latest .
docker push your-registry/aesthetic-scoring-service:latest
```

### Deploying to Kubernetes

```bash
# Deploy the application
kubectl apply -f immich/deploy/k8s/app/

# Deploy monitoring
kubectl apply -f immich/deploy/k8s/monitoring/

# Deploy autoscaling
kubectl apply -f immich/deploy/k8s/autoscaling/
```

### Running Ansible Playbooks

```bash
cd immich/deploy/ansible
ansible-playbook -i inventory.yml general/hello_host.yml
```

### Applying Terraform

```bash
cd immich/deploy/terraform/kvm
terraform init
terraform plan
terraform apply
```

## Database Setup

The database setup scripts are now located at:
- SQL script: `immich/deploy/scripts/create_immich_readonly_user.sql`
- Setup script: `immich/deploy/scripts/setup_immich_readonly.sh`

See `immich/deploy/scripts/README.md` for detailed instructions.

## Documentation

All aesthetic scoring documentation is now located in `immich/docs/aesthetic-scoring/`:
- Architecture overview
- Deployment guide (bringup)
- Infrastructure table
- Logging configuration
- Safeguarding practices

## Integration with Immich

The Immich server now includes the aesthetic integration module at:
`immich/server/src/modules/aesthetic-integration/`

This module provides:
- Webhook service for Feature Service integration
- Data Pipeline database connection
- Score retrieval and merging
- Admin API for batch rescoring

## Next Steps

1. Update CI/CD pipelines to reference new paths
2. Update any external documentation or runbooks
3. Update team knowledge base with new directory structure
4. Consider updating GitHub repository name if desired
