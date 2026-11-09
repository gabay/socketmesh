#!/bin/sh


docker buildx build -t gabay/socketmesh --platform linux/amd64,linux/arm64 --push .
