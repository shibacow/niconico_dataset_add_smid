# ニコニコデータセットのBigQueryへのロード

## 目的

ニコニコ動画のデータセットが[公開](https://www.nii.ac.jp/dsc/idr/nico/nico.html)された。
分析するために、BigQUeryへデータをロードする。

## 認証

環境変数　`GOOGLE_APPLICATION_CREDENTIALS`に ` gcloud auth application-default login` したあとの`config/gcloud/application_default_credentials.json` を指定する。

## 使い方

- 動画情報(Vide)のロードの[README](./video/README.md)
- コメント情報(comment)のロードの[README](./comment/README.md)
- コメント情報（旧版）のロードの[READMD](./comment_old/README.md)

## データロス率

今回は全てのデータがロードされた。


|       |元データ|BQロード|喪失レコード数|喪失率|
|:-----:|:-------------:|:------------:|:-------:|:---:|
|コメント|4,126,253,731|4,126,253,731|0|0%|
|動画情報|19,712,836|19,712,836|0|0.0%|


