#!/usr/bin/env sh
set -eu

mkdir -p mysql/data
curl -fL --retry 3 \
  -o mysql/data/athlete_event_results.csv \
  https://ftp.goit.study/neoversity/athlete_event_results.csv
