#!/bin/bash
# ============================================================================
# Setup Immich Read-Only Database User
# ============================================================================
# This script automates the creation of a read-only PostgreSQL user for
# Immich Server to query aesthetic scores from the Data Pipeline database.
#
# Usage: ./setup_immich_readonly.sh
#
# Requirements:
# - kubectl configured with access to the aesthetic-hub namespace
# - Data Pipeline PostgreSQL pod running (data-pipeline-postgres-0)
# - openssl for password generation
# ============================================================================

set -e  # Exit on error

# Configuration
NAMESPACE="aesthetic-hub"
POSTGRES_POD="data-pipeline-postgres-0"
POSTGRES_USER="aesthetic"
POSTGRES_DB="aesthetic_hub"
READONLY_USER="immich_readonly"
SECRET_NAME="data-pipeline-postgres-secret"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_SCRIPT="${SCRIPT_DIR}/create_immich_readonly_user.sql"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi
    
    # Check openssl
    if ! command -v openssl &> /dev/null; then
        log_error "openssl not found. Please install openssl."
        exit 1
    fi
    
    # Check if PostgreSQL pod exists
    if ! kubectl get pod "${POSTGRES_POD}" -n "${NAMESPACE}" &> /dev/null; then
        log_error "PostgreSQL pod '${POSTGRES_POD}' not found in namespace '${NAMESPACE}'."
        exit 1
    fi
    
    # Check if SQL script exists
    if [ ! -f "${SQL_SCRIPT}" ]; then
        log_error "SQL script not found: ${SQL_SCRIPT}"
        exit 1
    fi
    
    log_info "Prerequisites check passed."
}

# Generate password
generate_password() {
    log_info "Generating secure password..."
    READONLY_PASSWORD=$(openssl rand -base64 32)
    log_info "Password generated successfully."
}

# Create or update Kubernetes Secret
create_secret() {
    log_info "Creating/updating Kubernetes Secret..."
    
    # Check if secret exists
    if kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" &> /dev/null; then
        log_warn "Secret '${SECRET_NAME}' already exists. Updating..."
        kubectl create secret generic "${SECRET_NAME}" \
            --from-literal=immich_readonly_password="${READONLY_PASSWORD}" \
            --namespace="${NAMESPACE}" \
            --dry-run=client -o yaml | kubectl apply -f -
    else
        kubectl create secret generic "${SECRET_NAME}" \
            --from-literal=immich_readonly_password="${READONLY_PASSWORD}" \
            --namespace="${NAMESPACE}"
    fi
    
    log_info "Secret created/updated successfully."
}

# Apply SQL script
apply_sql_script() {
    log_info "Applying SQL script to create read-only user..."
    
    # Create temporary script with password
    TMP_SCRIPT="/tmp/create_immich_readonly_user_${RANDOM}.sql"
    sed "s/CHANGE_ME_PASSWORD/${READONLY_PASSWORD}/g" "${SQL_SCRIPT}" > "${TMP_SCRIPT}"
    
    # Copy script to pod
    kubectl cp "${TMP_SCRIPT}" "${NAMESPACE}/${POSTGRES_POD}:/tmp/create_immich_readonly_user.sql"
    
    # Execute script
    kubectl exec -it "${POSTGRES_POD}" -n "${NAMESPACE}" -- \
        psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f /tmp/create_immich_readonly_user.sql
    
    # Clean up
    rm -f "${TMP_SCRIPT}"
    kubectl exec "${POSTGRES_POD}" -n "${NAMESPACE}" -- rm -f /tmp/create_immich_readonly_user.sql
    
    log_info "SQL script applied successfully."
}

# Verify user creation
verify_user() {
    log_info "Verifying user creation..."
    
    # Check user exists
    log_info "Checking if user '${READONLY_USER}' exists..."
    kubectl exec "${POSTGRES_POD}" -n "${NAMESPACE}" -- \
        psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\du ${READONLY_USER}"
    
    # Check table permissions
    log_info "Checking table permissions..."
    kubectl exec "${POSTGRES_POD}" -n "${NAMESPACE}" -- \
        psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\dp aesthetic_scores"
    
    # Test read access
    log_info "Testing read access..."
    if kubectl exec "${POSTGRES_POD}" -n "${NAMESPACE}" -- \
        psql -U "${READONLY_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM aesthetic_scores;" &> /dev/null; then
        log_info "Read access verified successfully."
    else
        log_error "Read access test failed."
        exit 1
    fi
    
    # Test write access is denied
    log_info "Testing write access is denied..."
    if kubectl exec "${POSTGRES_POD}" -n "${NAMESPACE}" -- \
        psql -U "${READONLY_USER}" -d "${POSTGRES_DB}" \
        -c "INSERT INTO aesthetic_scores (asset_id, user_id, score) VALUES ('00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', 0.5);" 2>&1 | grep -q "permission denied"; then
        log_info "Write access correctly denied."
    else
        log_warn "Write access test did not return expected error."
    fi
    
    log_info "Verification completed successfully."
}

# Main execution
main() {
    log_info "Starting Immich read-only user setup..."
    echo ""
    
    check_prerequisites
    echo ""
    
    generate_password
    echo ""
    
    create_secret
    echo ""
    
    apply_sql_script
    echo ""
    
    verify_user
    echo ""
    
    log_info "Setup completed successfully!"
    log_info "The read-only user '${READONLY_USER}' has been created with SELECT permission on aesthetic_scores table."
    log_info "Password stored in Kubernetes Secret: ${SECRET_NAME}"
    echo ""
    log_info "Next steps:"
    echo "  1. Update Immich Server deployment to use the read-only credentials (Task 16.2)"
    echo "  2. Configure DataPipelineRepository to use the read-only user"
    echo "  3. Verify Immich Server can query aesthetic scores"
}

# Run main function
main
