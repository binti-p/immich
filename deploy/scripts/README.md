# Database Scripts

This directory contains SQL scripts for database initialization and configuration.

## Create Immich Read-Only User

### Overview

The `create_immich_readonly_user.sql` script creates a read-only PostgreSQL user for Immich Server to query aesthetic scores from the Data Pipeline database.

**User Details:**
- Username: `immich_readonly`
- Permissions: SELECT only on `aesthetic_scores` table
- Database: `aesthetic_hub` (Data Pipeline PostgreSQL)

### Prerequisites

- Access to the Data Pipeline PostgreSQL database
- PostgreSQL admin credentials (user with CREATEROLE privilege)
- Kubernetes cluster with Data Pipeline PostgreSQL deployed

### Application Methods

#### Method 1: Apply via kubectl exec (Recommended for Kubernetes)

```bash
# 1. Generate a strong password
export READONLY_PASSWORD=$(openssl rand -base64 32)

# 2. Store the password in Kubernetes Secret
kubectl create secret generic data-pipeline-postgres-secret \
  --from-literal=immich_readonly_password="${READONLY_PASSWORD}" \
  --namespace=aesthetic-hub \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Copy the SQL script to the PostgreSQL pod
kubectl cp immich/deploy/scripts/create_immich_readonly_user.sql \
  aesthetic-hub/data-pipeline-postgres-0:/tmp/create_immich_readonly_user.sql

# 4. Update the password in the script
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  sed -i "s/CHANGE_ME_PASSWORD/${READONLY_PASSWORD}/g" /tmp/create_immich_readonly_user.sql

# 5. Execute the SQL script
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U aesthetic -d aesthetic_hub -f /tmp/create_immich_readonly_user.sql

# 6. Verify the user was created
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U aesthetic -d aesthetic_hub -c "\du immich_readonly"

# 7. Verify permissions
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U aesthetic -d aesthetic_hub -c "\dp aesthetic_scores"
```

#### Method 2: Apply via psql (Direct Database Access)

```bash
# 1. Generate a strong password
export READONLY_PASSWORD=$(openssl rand -base64 32)

# 2. Update the password in the SQL script
sed "s/CHANGE_ME_PASSWORD/${READONLY_PASSWORD}/g" \
  immich/deploy/scripts/create_immich_readonly_user.sql > /tmp/create_immich_readonly_user.sql

# 3. Execute the SQL script
psql -h <DATA_PIPELINE_DB_HOST> \
     -p 5432 \
     -U aesthetic \
     -d aesthetic_hub \
     -f /tmp/create_immich_readonly_user.sql

# 4. Store the password in Kubernetes Secret
kubectl create secret generic data-pipeline-postgres-secret \
  --from-literal=immich_readonly_password="${READONLY_PASSWORD}" \
  --namespace=aesthetic-hub \
  --dry-run=client -o yaml | kubectl apply -f -
```

#### Method 3: Manual Application

```bash
# 1. Generate a strong password
openssl rand -base64 32

# 2. Connect to the database
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U aesthetic -d aesthetic_hub

# 3. Run the SQL commands manually, replacing CHANGE_ME_PASSWORD with your generated password
CREATE USER immich_readonly WITH PASSWORD 'your_generated_password';
GRANT CONNECT ON DATABASE aesthetic_hub TO immich_readonly;
GRANT USAGE ON SCHEMA public TO immich_readonly;
GRANT SELECT ON TABLE aesthetic_scores TO immich_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE aesthetic_scores FROM immich_readonly;

# 4. Verify
\du immich_readonly
\dp aesthetic_scores
\q

# 5. Store the password in Kubernetes Secret
kubectl create secret generic data-pipeline-postgres-secret \
  --from-literal=immich_readonly_password="your_generated_password" \
  --namespace=aesthetic-hub \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Verification

After applying the script, verify the user and permissions:

```bash
# Check user exists
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U aesthetic -d aesthetic_hub -c "\du immich_readonly"

# Expected output:
#                                    List of roles
#    Role name     |                         Attributes                         
# -----------------+------------------------------------------------------------
#  immich_readonly | 

# Check table permissions
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U aesthetic -d aesthetic_hub -c "\dp aesthetic_scores"

# Expected output should show:
# immich_readonly=r/aesthetic (r = SELECT permission)

# Test read access
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U immich_readonly -d aesthetic_hub -c "SELECT COUNT(*) FROM aesthetic_scores;"

# Test write access is denied (should fail)
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- \
  psql -U immich_readonly -d aesthetic_hub -c "INSERT INTO aesthetic_scores (asset_id, user_id, score) VALUES ('00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', 0.5);"

# Expected error: permission denied for table aesthetic_scores
```

### Security Notes

1. **Password Management**: The password should be stored securely in Kubernetes Secrets and never committed to version control
2. **Least Privilege**: The user has SELECT permission only on the `aesthetic_scores` table, following the principle of least privilege
3. **No Write Access**: The user cannot INSERT, UPDATE, DELETE, or TRUNCATE data
4. **Schema Access**: The user has USAGE permission on the public schema but cannot create or modify schema objects

### Troubleshooting

**Issue: User already exists**
```sql
-- Drop the existing user and recreate
DROP USER IF EXISTS immich_readonly;
-- Then run the create_immich_readonly_user.sql script again
```

**Issue: Permission denied when connecting**
```bash
# Verify the password is correct in the Kubernetes Secret
kubectl get secret data-pipeline-postgres-secret -n aesthetic-hub -o jsonpath='{.data.immich_readonly_password}' | base64 -d

# Verify pg_hba.conf allows connections from Immich Server pods
kubectl exec -it data-pipeline-postgres-0 -n aesthetic-hub -- cat /var/lib/postgresql/data/pg_hba.conf
```

**Issue: Table not found**
```sql
-- Verify the aesthetic_scores table exists
\c aesthetic_hub
\dt aesthetic_scores

-- If the table doesn't exist, create it first (see design.md for schema)
```

### Next Steps

After creating the read-only user:

1. Update Immich Server deployment to use the `immich_readonly` credentials (Task 16.2)
2. Configure DataPipelineRepository to use the read-only user
3. Verify Immich Server can query aesthetic scores
4. Validate credentials are not exposed in logs or API responses (Task 16.3)

### Related Tasks

- Task 16.1: Create read-only database user (this task)
- Task 16.2: Update database connection to use read-only credentials
- Task 16.3: Validate database credentials are not exposed
- Task 12.5: Create Kubernetes Secrets for database credentials

### References

- Requirements: 16.2 (Security and Access Control)
- Design Document: Section "Security Considerations"
- PostgreSQL Documentation: [GRANT](https://www.postgresql.org/docs/current/sql-grant.html)
