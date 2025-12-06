#!/bin/bash

echo "Stopping all running containers..."
docker stop $(docker ps -aq) 2>/dev/null

echo "Removing all containers..."
docker rm -f $(docker ps -aq) 2>/dev/null

echo "Removing all images..."
docker rmi -f $(docker images -q) 2>/dev/null

echo "Removing all volumes..."
docker volume rm $(docker volume ls -q) 2>/dev/null

echo "Removing all user-defined networks..."
docker network rm $(docker network ls | awk '$3 == "bridge" && $2 != "bridge" {print $1}') 2>/dev/null

echo "Docker cleanup complete!"
