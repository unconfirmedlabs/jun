#!/usr/bin/env bash
# bootstrap-clickhouse.sh — Self-hosted ClickHouse setup for jun (Sui indexer)
#
# Usage: ./bootstrap-clickhouse.sh <hostname> [tailscale-auth-key]
#
# Designed for beefy bare-metal machines (Hetzner AX102: 32c/128-249GB/NVMe).
# Tested on Ubuntu 22.04 / 24.04 and Debian 12.
#
# NOTE: If the jun repo (https://github.com/unconfirmedlabs/jun) is private,
# you must either:
#   a) Set up an SSH deploy key before running this script:
#      ssh-keygen -t ed25519 -f /root/.ssh/jun_deploy -N ""
#      # Add the public key to GitHub repo → Settings → Deploy keys
#   b) Replace JUN_REPO_URL below with git@github.com:unconfirmedlabs/jun.git
#      and place the private key at /root/.ssh/id_ed25519 before running.

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JUN_REPO_URL="https://github.com/unconfirmedlabs/jun.git"
JUN_INSTALL_DIR="/opt/jun"
JUN_BIN="/usr/local/bin/jun"
BUN_BIN="/root/.bun/bin/bun"
CLICKHOUSE_DATABASE="jun"
CLICKHOUSE_USER="default"

# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------

RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

log()  { printf "${GREEN}[%s] %s${NC}\n"  "$(date +%H:%M:%S)" "$*"; }
warn() { printf "${YELLOW}[%s] WARN: %s${NC}\n" "$(date +%H:%M:%S)" "$*"; }
err()  { printf "${RED}[%s] ERROR: %s${NC}\n"  "$(date +%H:%M:%S)" "$*" >&2; }

# ---------------------------------------------------------------------------
# Step 1: Validate arguments and root
# ---------------------------------------------------------------------------

if [[ $EUID -ne 0 ]]; then
  err "This script must be run as root (got EUID=$EUID)."
  exit 1
fi

if [[ $# -lt 1 ]]; then
  err "Usage: $0 <hostname> [tailscale-auth-key]"
  err "  hostname        Required. e.g. jun-chdb-slc1-primary"
  err "  tailscale-auth  Optional. tskey-auth-... from Tailscale admin console."
  exit 1
fi

HOSTNAME_NEW="$1"
TAILSCALE_AUTH_KEY="${2:-}"

log "Starting ClickHouse bootstrap for host: ${HOSTNAME_NEW}"
[[ -z "$TAILSCALE_AUTH_KEY" ]] && warn "No Tailscale auth key provided — manual enrollment instructions will be printed at the end."

# ---------------------------------------------------------------------------
# Step 2: Set hostname
# ---------------------------------------------------------------------------

log "Setting hostname to ${HOSTNAME_NEW}"
hostnamectl set-hostname "${HOSTNAME_NEW}"
# Also update /etc/hosts so 127.0.1.1 resolves the new hostname
if grep -q "^127\.0\.1\.1" /etc/hosts; then
  sed -i "s/^127\.0\.1\.1.*/127.0.1.1\t${HOSTNAME_NEW}/" /etc/hosts
else
  echo -e "127.0.1.1\t${HOSTNAME_NEW}" >> /etc/hosts
fi

# ---------------------------------------------------------------------------
# Step 3: Install base packages
# ---------------------------------------------------------------------------

log "Installing base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
  curl \
  wget \
  git \
  ufw \
  gnupg \
  ca-certificates \
  lsb-release \
  apt-transport-https \
  openssl \
  cpufrequtils \
  linux-tools-common \
  2>/dev/null || true   # cpufrequtils/linux-tools may not exist on all kernels; non-fatal

# ---------------------------------------------------------------------------
# Step 4: Kernel tuning
# ---------------------------------------------------------------------------

log "Applying kernel tuning"

# -- Transparent Huge Pages (THP) disable via systemd unit
cat > /etc/systemd/system/disable-thp.service <<'EOF'
[Unit]
Description=Disable Transparent Huge Pages
DefaultDependencies=no
After=sysinit.target local-fs.target
Before=basic.target

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/enabled'
ExecStart=/bin/sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/defrag'
RemainAfterExit=yes

[Install]
WantedBy=basic.target
EOF

systemctl daemon-reload
systemctl enable --now disable-thp.service

# Apply immediately (the unit may already be running but echo directly to be sure)
if [[ -f /sys/kernel/mm/transparent_hugepage/enabled ]]; then
  echo never > /sys/kernel/mm/transparent_hugepage/enabled || true
fi
if [[ -f /sys/kernel/mm/transparent_hugepage/defrag ]]; then
  echo never > /sys/kernel/mm/transparent_hugepage/defrag || true
fi

# -- sysctl tuning
cat > /etc/sysctl.d/99-clickhouse.conf <<'EOF'
vm.swappiness = 1
vm.overcommit_memory = 1
vm.max_map_count = 262144
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
fs.file-max = 6815744
fs.aio-max-nr = 1048576
EOF

sysctl -p /etc/sysctl.d/99-clickhouse.conf

# -- File descriptor limits for clickhouse user
cat > /etc/security/limits.d/clickhouse.conf <<'EOF'
clickhouse  soft  nofile  262144
clickhouse  hard  nofile  262144
EOF

# -- CPU governor → performance (best-effort; no error if governors not exposed)
log "Setting CPU governor to performance"
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  echo performance > "$gov" 2>/dev/null || true
done

# ---------------------------------------------------------------------------
# Step 5: Install Tailscale + bring up (BEFORE firewall)
# ---------------------------------------------------------------------------

log "Installing Tailscale"
if ! command -v tailscale &>/dev/null; then
  curl -fsSL https://tailscale.com/install.sh | sh
else
  log "Tailscale already installed, skipping install"
fi

log "Starting Tailscale daemon"
systemctl enable --now tailscaled

if [[ -n "$TAILSCALE_AUTH_KEY" ]]; then
  log "Bringing up Tailscale with auth key"
  tailscale up \
    --authkey="${TAILSCALE_AUTH_KEY}" \
    --hostname="${HOSTNAME_NEW}" \
    --ssh \
    --accept-routes \
    --accept-dns=false
else
  warn "No Tailscale auth key supplied."
  warn "Run manually after the script completes:"
  warn "  tailscale up --hostname=${HOSTNAME_NEW} --ssh --accept-routes --accept-dns=false"
fi

# ---------------------------------------------------------------------------
# Step 6: Install ClickHouse
# ---------------------------------------------------------------------------

log "Adding ClickHouse apt repository"
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
  | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

ARCH=$(dpkg --print-architecture)
echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${ARCH}] https://packages.clickhouse.com/deb stable main" \
  | tee /etc/apt/sources.list.d/clickhouse.list

apt-get update -qq

log "Installing clickhouse-server and clickhouse-client"
# Accept default password prompt non-interactively (we override it below)
DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-server clickhouse-client

# ---------------------------------------------------------------------------
# Step 6b: Generate ClickHouse password
# ---------------------------------------------------------------------------

log "Generating ClickHouse default user password"
CH_PASSWORD=$(openssl rand -base64 32 | tr -d '/+=')
CH_PASSWORD_SHA256=$(printf '%s' "${CH_PASSWORD}" | openssl dgst -sha256 | awk '{print $2}')

# ---------------------------------------------------------------------------
# Step 6c: Write ClickHouse configuration drop-ins
# ---------------------------------------------------------------------------

log "Writing ClickHouse config: listen.xml"
mkdir -p /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d

cat > /etc/clickhouse-server/config.d/listen.xml <<'EOF'
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <interserver_http_port>9009</interserver_http_port>
</clickhouse>
EOF

# -- Detect hardware for dynamic tuning
TOTAL_RAM_KB=$(awk '/^MemTotal:/{print $2}' /proc/meminfo)
TOTAL_RAM_BYTES=$(( TOTAL_RAM_KB * 1024 ))
CPU_COUNT=$(nproc)

# mark_cache_size = 5% of RAM, min 2 GiB
MARK_CACHE_BYTES=$(( TOTAL_RAM_BYTES / 20 ))
MIN_MARK_CACHE=$(( 2 * 1024 * 1024 * 1024 ))
if (( MARK_CACHE_BYTES < MIN_MARK_CACHE )); then
  MARK_CACHE_BYTES=$MIN_MARK_CACHE
fi

MAX_THREAD_POOL=$(( CPU_COUNT * 2 ))
BG_POOL_SIZE=${CPU_COUNT}

log "Hardware: ${CPU_COUNT} CPUs, $(( TOTAL_RAM_KB / 1024 / 1024 )) GiB RAM"
log "Performance tuning: mark_cache=${MARK_CACHE_BYTES} bytes, thread_pool=${MAX_THREAD_POOL}, bg_pool=${BG_POOL_SIZE}"

log "Writing ClickHouse config: performance.xml"
cat > /etc/clickhouse-server/config.d/performance.xml <<EOF
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>
    <mark_cache_size>${MARK_CACHE_BYTES}</mark_cache_size>
    <max_thread_pool_size>${MAX_THREAD_POOL}</max_thread_pool_size>
    <background_pool_size>${BG_POOL_SIZE}</background_pool_size>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>
    <max_concurrent_queries>200</max_concurrent_queries>
    <max_connections>4096</max_connections>
</clickhouse>
EOF

log "Writing ClickHouse config: logging.xml"
cat > /etc/clickhouse-server/config.d/logging.xml <<'EOF'
<clickhouse>
    <logger>
        <level>warning</level>
        <size>1000M</size>
        <count>7</count>
    </logger>
</clickhouse>
EOF

log "Writing ClickHouse users config: default password"
cat > /etc/clickhouse-server/users.d/default.xml <<EOF
<clickhouse>
    <users>
        <default>
            <password remove="1"/>
            <password_sha256_hex>${CH_PASSWORD_SHA256}</password_sha256_hex>
        </default>
    </users>
</clickhouse>
EOF

# Fix ownership so clickhouse user can read config
chown -R clickhouse:clickhouse /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d 2>/dev/null || true

# ---------------------------------------------------------------------------
# Step 7: Start ClickHouse + create jun database
# ---------------------------------------------------------------------------

log "Enabling and starting clickhouse-server"
systemctl enable clickhouse-server
systemctl restart clickhouse-server

# Wait for ClickHouse to be ready (up to 60s)
log "Waiting for ClickHouse to become ready"
CH_READY=0
for i in $(seq 1 60); do
  if clickhouse-client --password="${CH_PASSWORD}" --query="SELECT 1" &>/dev/null 2>&1; then
    CH_READY=1
    log "ClickHouse is ready (after ${i}s)"
    break
  fi
  sleep 1
done

if [[ $CH_READY -eq 0 ]]; then
  err "ClickHouse did not become ready within 60 seconds."
  err "Check logs: journalctl -u clickhouse-server -n 50"
  exit 1
fi

log "Creating database: ${CLICKHOUSE_DATABASE}"
clickhouse-client \
  --password="${CH_PASSWORD}" \
  --query="CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}"

# ---------------------------------------------------------------------------
# Step 8: Install Bun
# ---------------------------------------------------------------------------

log "Installing Bun"
if [[ -x "${BUN_BIN}" ]]; then
  log "Bun already installed at ${BUN_BIN}, skipping"
else
  curl -fsSL https://bun.sh/install | bash
fi

# Add Bun to PATH in bashrc and profile (idempotent)
BUN_PATH_EXPORT='export PATH="/root/.bun/bin:$PATH"'
for rc_file in /root/.bashrc /root/.profile; do
  if ! grep -qF '.bun/bin' "${rc_file}" 2>/dev/null; then
    echo "" >> "${rc_file}"
    echo "# Bun" >> "${rc_file}"
    echo "${BUN_PATH_EXPORT}" >> "${rc_file}"
  fi
done

# ---------------------------------------------------------------------------
# Step 9: Clone jun + create wrapper + write env
# ---------------------------------------------------------------------------

log "Setting up jun at ${JUN_INSTALL_DIR}"

if [[ -d "${JUN_INSTALL_DIR}/.git" ]]; then
  log "jun already cloned — pulling latest"
  git -C "${JUN_INSTALL_DIR}" pull
else
  log "Cloning jun from ${JUN_REPO_URL}"
  git clone "${JUN_REPO_URL}" "${JUN_INSTALL_DIR}"
fi

log "Installing jun dependencies"
"${BUN_BIN}" install --cwd "${JUN_INSTALL_DIR}"

log "Creating jun wrapper at ${JUN_BIN}"
cat > "${JUN_BIN}" <<EOF
#!/bin/bash
exec ${BUN_BIN} ${JUN_INSTALL_DIR}/src/cli.ts "\$@"
EOF
chmod +x "${JUN_BIN}"

# Write JUN_CLICKHOUSE_URL to /etc/environment (system-wide, survives reboots)
log "Writing JUN_CLICKHOUSE_URL to /etc/environment"
CH_URL="http://${CLICKHOUSE_USER}:${CH_PASSWORD}@localhost:8123"

# Remove stale entry if present, then append
if grep -q '^JUN_CLICKHOUSE_URL=' /etc/environment 2>/dev/null; then
  sed -i '/^JUN_CLICKHOUSE_URL=/d' /etc/environment
fi
echo "JUN_CLICKHOUSE_URL=${CH_URL}" >> /etc/environment

# ---------------------------------------------------------------------------
# Step 10: Enable firewall (LAST — Tailscale must already be up)
# ---------------------------------------------------------------------------

log "Configuring and enabling ufw firewall"

# Reset to defaults non-interactively
ufw --force reset

# Safety net: always allow SSH
ufw allow 22/tcp comment 'SSH fallback'

# Allow all traffic on the Tailscale interface
ufw allow in on tailscale0 comment 'Tailscale'
ufw allow out on tailscale0 comment 'Tailscale'

# Explicitly block ClickHouse ports from public internet
# (They're already blocked by default deny, but explicit is better)
ufw deny 8123/tcp comment 'Block ClickHouse HTTP from public'
ufw deny 9000/tcp comment 'Block ClickHouse native from public'
ufw deny 9009/tcp comment 'Block ClickHouse interserver from public'

# Default policies
ufw default deny incoming
ufw default allow outgoing

# Enable non-interactively
ufw --force enable

log "ufw status:"
ufw status verbose

# ---------------------------------------------------------------------------
# Step 11: Print summary
# ---------------------------------------------------------------------------

TAILSCALE_IP="(not available — run: tailscale ip -4)"
if command -v tailscale &>/dev/null && tailscale status &>/dev/null 2>&1; then
  TAILSCALE_IP=$(tailscale ip -4 2>/dev/null || echo "(not available — run: tailscale ip -4)")
fi

TAILSCALE_CONNECT_URL="http://${CLICKHOUSE_USER}:${CH_PASSWORD}@${HOSTNAME_NEW}:8123"
TAILSCALE_IP_URL="http://${CLICKHOUSE_USER}:${CH_PASSWORD}@${TAILSCALE_IP}:8123"

printf "\n"
printf "${GREEN}======================================================${NC}\n"
printf "${GREEN}  Bootstrap complete: ${HOSTNAME_NEW}${NC}\n"
printf "${GREEN}======================================================${NC}\n"
printf "\n"
printf "  %-28s %s\n" "Hostname:"           "${HOSTNAME_NEW}"
printf "  %-28s %s\n" "Tailscale IP:"       "${TAILSCALE_IP}"
printf "\n"
printf "  %-28s %s\n" "ClickHouse database:" "${CLICKHOUSE_DATABASE}"
printf "  %-28s %s\n" "ClickHouse user:"     "${CLICKHOUSE_USER}"
printf "  %-28s %s\n" "ClickHouse password:" "${CH_PASSWORD}"
printf "\n"
printf "${YELLOW}  NOTE: This is the only time the password will be shown.${NC}\n"
printf "${YELLOW}        Save it somewhere secure now.${NC}\n"
printf "\n"
printf "  %-28s %s\n" "Connect via Tailscale hostname:" "${TAILSCALE_CONNECT_URL}"
printf "  %-28s %s\n" "Connect via Tailscale IP:"       "${TAILSCALE_IP_URL}"
printf "\n"
printf "  jun CLI usage (env var already set in /etc/environment):\n"
printf "    source /etc/environment && jun index replay-chain ...\n"
printf "\n"
printf "  Or pass explicitly:\n"
printf "    jun index replay-chain --clickhouse '${CH_URL}' ...\n"
printf "\n"

if [[ -z "$TAILSCALE_AUTH_KEY" ]]; then
  printf "${YELLOW}  Tailscale not yet enrolled. Run from this machine:${NC}\n"
  printf "${YELLOW}    tailscale up --hostname=${HOSTNAME_NEW} --ssh --accept-routes --accept-dns=false${NC}\n"
  printf "\n"
fi

printf "${GREEN}  ClickHouse status:${NC}\n"
printf "    systemctl status clickhouse-server\n"
printf "    journalctl -u clickhouse-server -f\n"
printf "\n"
printf "${GREEN}======================================================${NC}\n"
