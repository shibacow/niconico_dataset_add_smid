#!/bin/bash
#echo "select * from nico_data2.comment_restore" | bq query  --use_legacy_sql=false  --time_partitioning_type=MONTH  --time_partitioning_field=created_at --destination_table=nico_data.comment
#echo "select * from nico_data.comment where date(created_at,'Japan') between date('2021-01-01') and date('2021-01-15')" | bq query --use_legacy_sql=false  --time_partitioning_type=DAY  --time_partitioning_field=created_at --destination_table=nico_data.comment_sample
#echo "select * from nico_data.video where date(upload_time,'Japan') between date('2021-01-01') and date('2021-01-15')" | bq query --use_legacy_sql=false  --time_partitioning_type=DAY  --time_partitioning_field=upload_time --destination_table=nico_data.video_sample


