set -e
uv run update.py
exec uv run -m Backend
