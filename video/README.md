# 動画情報のロード

### 動画情報の変換

まず最初に、`video` フォルダに、NIIから落としてきたvideoのzipを格納する。その際に `video_dst` フォルダーを作る。

```py
python3 video_convert.py
````

を実行すれば、json.gz が作られる。それを `GCS`  へアップロードし、

```sh
bq load --source_format=NEWLINE_DELIMITED_JSON nico_data_2018.video "gs://foo-bar/video/*.json.gz" video.json
```

とすれば良い。
