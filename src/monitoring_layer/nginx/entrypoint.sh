#!/bin/sh

set -e

# Authentication lives at the Caddy reverse proxy so nginx only serves static files.
exec nginx -g 'daemon off;'
