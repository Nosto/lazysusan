#!/bin/bash

docker run --env GITHUB_WORKSPACE=/app --volume=/Users/mridang/Junk/inspect/:/out --volume=$(pwd)/.:/app docker.pkg.github.com/mridang/action-idea/idealize:latest /app /app/.idea/inspectionProfiles/CI.xml /out v2 Inspection
