#!/bin/sh

cargo build --release
mv target/release/rusty_knowgly .

ENDPOINT="http://fedora:8888"

mkdir -p temp

# Cluster
# 3 clusters
echo "Creating VDoc templates for 3 fields..."
RUST_BACKTRACE=1 ./rusty_knowgly cluster-metric-results --calculated-metrics-file temp/entity_type_importance_metrics.json --n-clusters 3 --output temp/entity_type_importance_metrics_clustered_3.json
RUST_BACKTRACE=1 ./rusty_knowgly cluster-metric-results --calculated-metrics-file temp/entropy_type_importance_metrics.json --n-clusters 3 --output temp/entropy_type_importance_metrics_clustered_3.json
# 5 clusters
echo "Creating VDoc templates for 5 fields..."
RUST_BACKTRACE=1 ./rusty_knowgly cluster-metric-results --calculated-metrics-file temp/entity_type_importance_metrics.json --n-clusters 5 --output temp/entity_type_importance_metrics_clustered_5.json
RUST_BACKTRACE=1 ./rusty_knowgly cluster-metric-results --calculated-metrics-file temp/entropy_type_importance_metrics.json --n-clusters 5 --output temp/entropy_type_importance_metrics_clustered_5.json

# Create single-field VDocs for all LaQuE entities. This allows us to "reshuffle" them with the different vdocs afterwards
#
# Trick: We tell it to cluster into single-field VDocs using any of the metrics
echo "Creating base single-field VDocs..."
RUST_BACKTRACE=1 ./rusty_knowgly cluster-metric-results --calculated-metrics-file temp/entity_type_importance_metrics.json --n-clusters 1 --output temp/single_field_vdoc_templates.json
RUST_BACKTRACE=1 ./rusty_knowgly index-from-entity-iris --entity-iris-file LaQuE/LaQuE_collection.tsv --clustered-metrics-file temp/single_field_vdoc_templates.json --output temp/single_field_vdocs.json --endpoint "$ENDPOINT"

# Reuse the single-field VDocs to create {entity_type_importance, entropy_type_importance} VDocs with {3,5} fields
# 3 fields
echo "Creating VDocs for 3 fields..."
RUST_BACKTRACE=1 ./rusty_knowgly index-from-extracted-entities --extracted-entities-file temp/single_field_vdocs.json --clustered-metrics-file temp/entity_type_importance_metrics_clustered_3.json --output entity_type_importance_metrics_vdocs_3.json
RUST_BACKTRACE=1 ./rusty_knowgly index-from-extracted-entities --extracted-entities-file temp/single_field_vdocs.json --clustered-metrics-file temp/entropy_type_importance_metrics_clustered_3.json --output entropy_type_importance_metrics_vdocs_3.json
# 5 fields
echo "Creating VDocs for 5 fields..."
RUST_BACKTRACE=1 ./rusty_knowgly index-from-extracted-entities --extracted-entities-file temp/single_field_vdocs.json --clustered-metrics-file temp/entity_type_importance_metrics_clustered_5.json --output entity_type_importance_metrics_vdocs_5.json
RUST_BACKTRACE=1 ./rusty_knowgly index-from-extracted-entities --extracted-entities-file temp/single_field_vdocs.json --clustered-metrics-file temp/entropy_type_importance_metrics_clustered_5.json --output entropy_type_importance_metrics_vdocs_5.json
