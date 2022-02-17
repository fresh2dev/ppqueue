#!/usr/bin/env sh

command_exists() {
  command -v $1 &> /dev/null
}

ADDR="$1"

if [ -z "$ADDR" ]; then
  echo "must specify an endpoint."
  exit -1
fi

if command_exists 'curl'; then
  curl -fsSL "$ADDR" >/dev/null
elif command_exists 'wget'; then
  wget --no-verbose --tries=1 --spider "$ADDR" >/dev/null
else
  echo 'curl, wget missing.'
  exit 1
fi

echo "service reachable at: $ADDR"
