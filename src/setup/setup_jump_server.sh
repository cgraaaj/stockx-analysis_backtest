#!/bin/bash
#
# Setup Script for Data Collection on Jump Server
# Run this on the new jump-server VM after copying files
#
# Usage: ./setup_jump_server.sh
#

set -e

echo "=============================================="
echo "  CGR-TRADES Data Collection Setup"
echo "  Jump Server Configuration"
echo "=============================================="

# Configuration
PROJECT_DIR="$HOME/cgr-trades"
PYTHON_VERSION="python3"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${GREEN}[✓]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; exit 1; }

# Step 1: Create directory structure
log "Creating project directory structure..."
mkdir -p "$PROJECT_DIR"/{src/data_collection,python,logs,requirements}

# Step 2: Check if files exist
if [ ! -f "$PROJECT_DIR/src/data_collection/opt-stk-data-to-db-upstox.py" ]; then
    error "Main script not found! Please copy files first using SCP."
fi

# Step 3: Install system dependencies
log "Installing system dependencies..."
sudo apt update
sudo apt install -y python3 python3-pip python3-venv libpq-dev

# Step 4: Create Python virtual environment
log "Creating Python virtual environment..."
cd "$PROJECT_DIR"
$PYTHON_VERSION -m venv sa
source sa/bin/activate

# Step 5: Install Python dependencies
log "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements/requirements.txt

# Step 6: Create .env file if not exists
if [ ! -f "$PROJECT_DIR/.env" ]; then
    log "Creating .env file..."
    cat > "$PROJECT_DIR/.env" << 'ENVEOF'
# Database Configuration for Jump Server
# Connects to PostgreSQL VM (192.168.1.40)

DB_USER=sd_admin
DB_PASSWORD=sdadmin@postgres
DB_HOST=192.168.1.40
DB_PORT=5432
DB_NAME=stock-dumps

# API Configuration
STOCK_SVC_URL=https://api.example.com/stock
ENVEOF
    log ".env file created - please verify settings!"
else
    log ".env file already exists"
fi

# Step 7: Test database connection
log "Testing database connection..."
source sa/bin/activate
python3 << 'PYEOF'
import sys
sys.path.insert(0, '.')
from db_config import DB_CONNECTION_STRING
from sqlalchemy import create_engine, text

try:
    engine = create_engine(DB_CONNECTION_STRING)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM options.stock")).scalar()
        print(f"✓ Database connection successful! Found {result} stocks.")
except Exception as e:
    print(f"✗ Database connection failed: {e}")
    sys.exit(1)
PYEOF

# Step 8: Create cron job script
log "Creating daily data collection script..."
cat > "$PROJECT_DIR/src/data_collection/daily_data_collector.sh" << 'CRONEOF'
#!/bin/bash
# Daily Data Collection Script
# Run via cron: 0 7 * * 1-5 /path/to/daily_data_collector.sh

set -e

PROJECT_DIR="$HOME/cgr-trades"
LOG_DIR="$PROJECT_DIR/logs"
DATE=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/daily_data_collector_$(date +%Y%m%d_%H%M%S).log"

cd "$PROJECT_DIR"
source sa/bin/activate

echo "Starting data collection for $DATE" >> "$LOG_FILE"
python3 src/data_collection/opt-stk-data-to-db-upstox.py "$DATE" >> "$LOG_FILE" 2>&1
echo "Data collection completed" >> "$LOG_FILE"
CRONEOF

chmod +x "$PROJECT_DIR/src/data_collection/daily_data_collector.sh"

# Step 9: Setup cron job
log "Setting up cron job..."
CRON_CMD="30 7 * * 1-5 cd $PROJECT_DIR && source sa/bin/activate && python3 src/data_collection/opt-stk-data-to-db-upstox.py \$(date +\%Y-\%m-\%d) >> logs/cron_\$(date +\%Y\%m\%d).log 2>&1"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "opt-stk-data-to-db-upstox"; then
    log "Cron job already exists"
else
    (crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
    log "Cron job added: Runs Mon-Fri at 7:30 AM"
fi

echo ""
echo "=============================================="
echo "  Setup Complete!"
echo "=============================================="
echo ""
echo "Files location: $PROJECT_DIR"
echo "Virtual env: $PROJECT_DIR/sa"
echo "Logs: $PROJECT_DIR/logs"
echo ""
echo "To test manually:"
echo "  cd $PROJECT_DIR"
echo "  source sa/bin/activate"
echo "  python3 src/data_collection/opt-stk-data-to-db-upstox.py \$(date +%Y-%m-%d)"
echo ""
echo "Cron schedule: Mon-Fri at 7:30 AM IST"
echo "=============================================="

