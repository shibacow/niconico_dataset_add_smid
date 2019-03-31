# ニコニコデータセット変換ツール
## 目的

これは、ニコニコ動画のコメントデータセットに対して、smidを追加し、zipファイルから BigQueyにロードしやすいように、json.gzへ変更する。


## 使い方

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


## 補足

コメントデータには、vposがintの値を超えているデータが存在し、そのため、まともに入らないデータもあった。

元データの件数と、BigQueryに入った件数を比べると以下の通りであった。
喪失率は、それぞれ、コメントが `0.002%` 動画情報は `0.09%` なのでだいたい入ったと考える。

|       |元データ|BQロード|喪失レコード数|喪失率|
|:-----:|:-------------:|:------------:|:-------:|:---:|
|コメント|3,773,083,461|3,772,018,854|1,064,607|0.02821583%|
|動画情報|16,703,325|16,687,315|16,010|0.09584918%|


