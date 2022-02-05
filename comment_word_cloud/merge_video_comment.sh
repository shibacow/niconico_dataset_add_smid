#!/bin/bash
bq query --use_legacy_sql=false \
--time_partitioning_type=MONTH \
--time_partitioning_field=created_at \
--destination_table nico_data.comment_video \
--replace \
< merge_video_comment.sql
