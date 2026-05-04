#!/bin/bash
set -e

echo "Stopping all services..."
docker-compose down

echo "✅ All services stopped."
