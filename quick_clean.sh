#!/usr/bin/env bash

files=()
counter=0

MATCHES_REGEX='/Image/,/Image/p'
PATHS_REGEX='(^\/.*$)'

MATCHES=$(sed -n "$MATCHES_REGEX" $1)
PATHS=$(grep -Po "$PATHS_REGEX" <<<$MATCHES)

for entry in $PATHS; do
    files+=("$entry")
    ((counter++))


    if (( $counter > 10 )); then
        eog ${files[*]}
        files=()
    fi
done
eog ${files[*]}
