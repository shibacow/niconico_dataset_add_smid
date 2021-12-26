#!/bin/bash

# --flexRSGoal=SPEED_OPTIMIZED 
# --flexRSGoal=COST_OPTIMIZED
# --workerMachineType=n1-highmem-16
# --workerMachineType=n1-standard-2


mvn compile exec:java -Dexec.mainClass=com.shibacow.nico.NicoCommentLoad \
     -Dexec.args="--inputFile=gs://nico-comment-test/comment/0000.zip \
     --runner=DataflowRunner \
     --output=nico_test.comment_test  \
     --project=graphical-elf-807 \
     --flexRSGoal=SPEED_OPTIMIZED \
     --numWorkers=50 \
     --workerMachineType=n1-standard-2 \
     --tempLocation=gs://nico-data-tmp-gcs/tmp \
     --region=us-central1  \
     --stagingLocation=gs://nico-data-tmp-gcs/staging/ \
     " \
     -Pdataflow-runner
 
