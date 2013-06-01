# ニコニコデータセット変換ツール
## AIM
これは、ニコニコ動画のコメントデータセットに対して、smidを追加し、加えて、tar.gzからhadoopのSequenceFileに変換するツールである。


##使い方
コメントデータを、comment_srcと言うフォルダーを作って保存。また、comment_seqと言うフォルダーを作っておく。
`tar_to_seq_conv.py`
を実行すれば、変換が始まる。
###必要なライブラリ
- python
 threadpool

- java
