#!/bin/bash
set -e

echo "Testing environment setup..."
echo ""

echo "Docker:"
docker --version
docker-compose version
echo ""

echo "✅ All tools are working!"
echo ""
echo "Creating output directories..."
mkdir -p output logs
echo ""

echo "Starting for all images:"
echo "  docker-compose up -d"
