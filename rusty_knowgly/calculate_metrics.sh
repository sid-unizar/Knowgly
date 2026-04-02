#!/bin/sh

cargo build --release
mv target/release/rusty_knowgly .

ENDPOINT="http://fedora:8888"

mkdir -p temp

# Calculate metrics
RUST_BACKTRACE=1 ./rusty_knowgly calculate-metrics --endpoint "$ENDPOINT" --output temp/entity_type_importance_metrics.json --metric-to-calculate entity_type_importance
RUST_BACKTRACE=1 ./rusty_knowgly calculate-metrics --endpoint "$ENDPOINT" --output temp/entropy_type_importance_metrics.json --metric-to-calculate entropy_type_importance