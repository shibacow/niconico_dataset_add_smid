# コメントをBigQueryへロードする

dataflowを使って、ニコニコ動画のコメントをロードするシステム。

# jdk maven インストール

```
sudo apt install default-jdk
sudo apt install maven
```

# 環境変数の設定

環境変数　`GOOGLE_APPLICATION_CREDENTIALS` に `gcloud auth application-default login` したあとの `~/.config/gcloud/application_default_credentials.json` を指定する。

# 使い方

`load.sh`

## shellの説明

```sh
#!/bin/bash

# --flexRSGoal=SPEED_OPTIMIZED 
# --flexRSGoal=COST_OPTIMIZED
# --workerMachineType=n1-highmem-16
# --workerMachineType=n1-standard-2
# --numWorkers=50 

mvn compile exec:java -Dexec.mainClass=com.shibacow.nico.NicoCommentLoad \
     -Dexec.args="--inputFile=gs://<your_gcs_bucket>/comment/*.zip \
     --runner=DataflowRunner \
     --output=<your_dataset.table_name>  \
     --project=<your_project_id> \
     --flexRSGoal=SPEED_OPTIMIZED \
     --workerMachineType=n1-standard-2 \
     --tempLocation=gs://<your_gcs_bucket_tmp>/tmp \
     --region=us-central1  \
     --stagingLocation=gs://<your_gcs_bucket>/staging/ \
     " \
     -Pdataflow-runner
 

```
