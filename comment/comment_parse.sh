#!/bin/bash

# --flexRSGoal=SPEED_OPTIMIZED 
# --flexRSGoal=COST_OPTIMIZED
# --workerMachineType=n1-highmem-16
# --workerMachineType=n1-standard-2
# --numWorkers=50 

#mvn compile exec:java -Dexec.mainClass=com.shibacow.nico.NicoCommentLoad \
#     -Dexec.args="--inputTable=nico_data.comment_sample \
#     --runner=DataflowRunner \
#     --output=<your_dataset.table_name>  \
#     --project=<your_project_id> \
#     --flexRSGoal=SPEED_OPTIMIZED \
#     --workerMachineType=n1-standard-2 \
#     --tempLocation=gs://<your_gcs_bucket_tmp>/tmp \
#     --region=us-central1  \
#     --stagingLocation=gs://<your_gcs_bucket>/staging/ \
#     " \
#    -Pdataflow-runner
 
mvn compile exec:java -Dexec.mainClass=com.shibacow.nico.CommentParse \
     -Dexec.args="--commentTable=nico_data.comment_sample \
     --videoTable=nico_data.video_sample \
     --runner=DirectRunner \
     --outputFile=test-  \
     " \
    -Pdirect-runner
 
