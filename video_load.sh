#/bin/bash
#bq load --source_format=NEWLINE_DELIMITED_JSON nico_data_2018.video "gs://nicodata-load-2018/video/*.json.gz" video.json
bq load --max_bad_records=1000 --source_format=NEWLINE_DELIMITED_JSON nico_data_2018.comment "gs://nicodata-load-2018/comment/*.json.gz" comment.json
