# ニコニコデータセット変換ツール
## AIM
これは、ニコニコ動画のコメントデータセットに対して、smidを追加し、加えて、tar.gzからhadoopのSequenceFileに変換するツールである。


##使い方
コメントデータを、comment_srcと言うフォルダーを作って保存。また、comment_seqと言うフォルダーを作っておく。
`tar_to_seq_conv.py`
を実行すれば、変換が始まる。

##実行結果
元データ
    {"date":1175712657,"no":1,"vpos":768,"comment":"\uff11","command":""}
    {"date":1175712661,"no":2,"vpos":3208,"comment":"\u30d0\u30fc\u30ed\u30fc\u30fbu30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb","command":""}
    {"date":1175712664,"no":3,"vpos":8657,"comment":"\u30a2\u30ea\u30d7\u30ed\u304cu51fa\u3066\u304f\u308b\u3053\u3068\u3092\u9858\u3063\u3066\u308b\u304c\u2026","command":""}

編集データ
    {"vpos":768,"no":1,"command":"","filename":"comment_src\/0011\/sm110003.dat","video_id":"sm110003","comment":"１","date":1175712657}
    {"vpos":3208,"no":2,"command":"","filename":"comment_src\/0011\/sm110003.dat","video_id":"sm110003","comment":"バーロー・・ ・・・・・・・・・・・・・・・・・・・・","date":1175712661}
    {"vpos":8657,"no":3,"command":"","filename":"comment_src\/0011\/sm110003.dat","video_id":"sm110003","comment":"アリプロが出 てくることを願ってるが\u2026","date":1175712664}
    {"vpos":12763,"no":4,"command":"","filename":"comment_src\/0011\/sm110003.dat","video_id":"sm110003","comment":"乾いた叫びがーーー","date":1175712690}

video_id,filenameが追加されている。


###必要なライブラリ
- python
 threadpool

- java
