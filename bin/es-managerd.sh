#!/bin/sh

# Note: es-run-service must be in path
# Note: ESLIB_SERVICE_DIR must be set, or -d option must be used
exec ./es-service $@ -c manager managerd -e localhost:5000 --start
