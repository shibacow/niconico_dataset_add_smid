/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shibacow.nico;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class CommentParse {

  public interface NicoLoadOptions extends PipelineOptions {

    @Description("comment table")
    @Required
    String getCommentTable();
    void setCommentTable(String value);

    @Description("video table")
    @Required
    String getVideoTable();
    void setVideoTable(String value);

    @Description("gcs path")
    @Required
    String getOutputFile();
    void setOutputFile(String value);
  }
  static void runNicoLoad(NicoLoadOptions options) {
    Logger LOG = LoggerFactory.getLogger(CommentParse.class);
    LOG.info("commnet="+options.getCommentTable()+"\tvideo="+options.getVideoTable()+"\toutput="+options.getOutputFile());
    Pipeline p = Pipeline.create(options);
    PCollection<TableRow> videorowFromBigQuery = p.apply(
        BigQueryIO.readTableRows()
        .from(options.getVideoTable())
        .withMethod(Method.DIRECT_READ)
        .withSelectedFields(Arrays.asList("category","video_id")));
    PCollection<TableRow> commentFromBigQuery = p.apply(
        BigQueryIO.readTableRows()
        .from(options.getCommentTable())
        .withMethod(Method.DIRECT_READ)
        .withSelectedFields(Arrays.asList("video_id","created_at","content")));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    NicoLoadOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(NicoLoadOptions.class);
    runNicoLoad(options);
  }
}
