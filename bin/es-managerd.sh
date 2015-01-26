#!/bin/sh

# Note: es-run-service must be in path
# Note: ESLIB_SERVICE_DIR must be set, or -d option must be used
exec ./es-run-service $@ -c manager managerd ServiceManager
