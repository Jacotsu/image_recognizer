#!/usr/bin/env bash

counter=0
files[0]=''
files[1]=''


for entry in $(grep -A 2 "INFO:root:Similar images found" $1); do
    if [ -f "$entry" ]; then
        files[counter]="$entry"
        ((counter++))
    fi

    if (( counter >= 2 )); then
        eog "${files[0]}" "${files[1]}"
        counter=0
    fi

done
