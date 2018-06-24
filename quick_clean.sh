#!/usr/bin/env bash

files=()


MATCHES_REGEX='/Image/,/Image/p'
PATHS_REGEX='(^\/.*$)'

MATCHES=$(sed -n "$MATCHES_REGEX" $1)
PATHS=$(grep -Po "$PATHS_REGEX" <<<$MATCHES)

for entry in $PATHS; do
    files+=("$entry")
done
eog ${files[*]}
