# コメント情報(旧版)

手元で、`comment_convert.py` を使って、動画IDをjsonに含める方式。
dataflowを使って変換する方式に変えたため、使わなくなりました。

### コメント情報の変換

まず最初に、`comment` フォルダに、NIIから落としてきたcommentのzipを格納する。その際に `comment_dst` フォルダーを作る。

```py
python3 comment_convert.py
````

を実行すれば、json.gz が作られる。処理時間は、CPU 36core メモリ 24GBのマシンで、２時間程度だった。 。それを `GCS`  へアップロードし、


```sh
bq load --max_bad_records=1000 --source_format=NEWLINE_DELIMITED_JSON nico_data_2018.comment "gs://foo-bar/comment/*.json.gz" comment.json
```

とすれば良い。私が試したときは、コメントのロードは、20分程度で終わった。
