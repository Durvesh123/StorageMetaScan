set -e
echo "🔹 Activating Superset virtual environment..."
source /opt/superset-venv/bin/activate

export FLASK_APP=superset
cd /root/superset || { echo "❌ Superset directory not found!"; exit 1; }

echo "🔹 Upgrading Superset DB..."
superset db upgrade || echo "⚠️ Skipping DB upgrade (failed or not required)."

echo "🔹 Initializing Superset..."
superset init || echo "⚠️ Skipping init (already done)."

echo "🚀 Starting Apache Superset on http://0.0.0.0:8088 ..."
if superset --help | grep -q "run"; then
    # Old Superset version
    superset run -h 0.0.0.0 -p 8088
else
    # New Superset version
    gunicorn -w 4 -k gevent --timeout 120 -b 0.0.0.0:8088 "superset.app:create_app()"
fi
