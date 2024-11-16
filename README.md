# aws-athena-cost-attribution
Athena does not provide per query cost attribution. This is my attempt to create this via Spark processing Cloudtrail events and writing an Iceberg table.

Edit from the future:  and I learned how painful processing Cloudtrail events with Spark is - the JSON schemas can be very inconsistent! I was introduced to this alternative cloud-native approach which has been working very well for me in production https://aws.amazon.com/blogs/big-data/auditing-inspecting-and-visualizing-amazon-athena-usage-and-cost/

The process is to get Athena query IDs from Cloudtrail and fetch metadata like IAM role, TB scanned, start/end time from the Athena API https://docs.aws.amazon.com/cli/latest/reference/athena/batch-get-query-execution.html

Yes, I wish AWS made this easier :P

Development environment is docker-compose from https://github.com/tabular-io/docker-spark-iceberg
