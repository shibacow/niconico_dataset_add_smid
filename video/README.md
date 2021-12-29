# 動画情報のロード

### 動画情報の変換

まず最初に、`video` フォルダに、NIIから落としてきたvideoのjsonlを格納する。その際に `video_dst` フォルダーを作る。
jsonlはgz化したほうが使い勝手が良いため、`gzip *.jsonl` でjsonl.gz化する。
jsonl.gzを読み込む。tagsはarrayで入れたかったため `d['tags']=d['tags'].split(' ')` でtagsを配列化する。

```py
python3 video_convert.py
````

を実行すれば、json.gz が作られる。

### GCSへデータをアップロードする

```sh
gsutil -m cp *.jsonl.gz gs://<foo-bar>/video/
```


###  GCSからbigqueryへデータをロードする


それを `GCS`  へアップロードし、

```sh
bq load --source_format=NEWLINE_DELIMITED_JSON --time_partitioning_type=MONTH  --time_partitioning_field=upload_time  nico_test.video "gs://<foo-bar>/video/*.jsonl.gz" video.json
```

とすれば良い。
