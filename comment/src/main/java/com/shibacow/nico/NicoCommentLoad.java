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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.io.InputStreamReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.BufferedInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class NicoCommentLoad {

  public static class Comment{
    public String date;
    public String content;
    public String command;
    public long vpos;
    public boolean easy;
    public boolean owner;
    public Comment() {
    }
  }
  static class CommentExtractInZipFn extends DoFn<ReadableFile, String> {
    private final Counter zipCount = Metrics.counter(CommentExtractInZipFn.class, "zipCount");

    @ProcessElement
    public void processElement(@Element ReadableFile f,OutputReceiver<String> receiver) {
      Logger LOG = LoggerFactory.getLogger(CommentExtractInZipFn.class);
      String filename = f.getMetadata().resourceId().toString();
      LOG.info("filename="+filename+"\tf="+f);
      try{
        zipCount.inc();
        ReadableByteChannel readableByteChannel = FileSystems.open(f.getMetadata().resourceId());
        ZipInputStream zipInputStream= new ZipInputStream(Channels.newInputStream(readableByteChannel));
        ZipEntry zipEntry = null;
        while ((zipEntry = zipInputStream.getNextEntry()) != null) {
          String[] zip_names = zipEntry.getName().split("/");
          if(zip_names.length!=2){
            LOG.info("zip_name="+zipEntry.getName());
            continue;
          }
          String video_id_seed = zip_names[1];
          String video_id = video_id_seed.split("\\.")[0];
          BufferedInputStream bis = new BufferedInputStream(zipInputStream);
          BufferedReader r = new BufferedReader(new InputStreamReader(bis,"UTF-8"));
          r.lines().forEach(json -> {
            String s=video_id+"_"+json;
            receiver.output(s);
          });
        }
      }catch(java.io.IOException e){
       LOG.error("io error e="+e);
      }
    }
  }
  static class ConvertJsonToTableRowFn extends DoFn<String, TableRow>{
    private final Counter jsonLines = Metrics.counter(ConvertJsonToTableRowFn.class, "jsonlines");
    private final Distribution commentDist =
        Metrics.distribution(ConvertJsonToTableRowFn.class, "commentDIst");

    @ProcessElement
    public void processElement(@Element String str,OutputReceiver<TableRow> receiver) {       
        Logger LOG = LoggerFactory.getLogger(ConvertJsonToTableRowFn.class);
        String[] ss=str.split("_",2);
        if(ss.length==2){
          String video_id=ss[0];
          String json=ss[1];
          try{
            jsonLines.inc();
            ObjectMapper mapper = new ObjectMapper();
            Comment comment = mapper.readValue(json, Comment.class);
            commentDist.update(comment.content.length());
            TableRow row = new TableRow();
            row.set("video_id",video_id);
            row.set("created_at",comment.date);
            row.set("vpos",comment.vpos);
            int easy_f=0;
           if((boolean) comment.easy==true){
              easy_f=1;
            }
            int owner_f=0;
            if((boolean) comment.owner==true){
              owner_f=1;
            }
            row.set("easy",easy_f);
            row.set("owner",owner_f);
            if(comment.command!=""){
              row.set("command",comment.command);
            }else{
              row.set("command",null);
            }
            row.set("content",comment.content);
            receiver.output(row);
          }catch(JsonProcessingException e){
            LOG.error("jackson error="+e);
          }
        }else{
          LOG.error("length error="+Arrays.toString(ss));
        }
    }
  }
  

  static class ZipToTableRow extends PTransform<PCollection<ReadableFile>, PCollection<TableRow>> {
    @Override
    public PCollection<TableRow> expand(PCollection<ReadableFile> zip_file) {
      PCollection<String> results0 = zip_file.apply(ParDo.of(new CommentExtractInZipFn()));
      PCollection<TableRow> results = results0.apply(ParDo.of(new  ConvertJsonToTableRowFn()));
      return results;
    }
  }

  public interface NicoLoadOptions extends PipelineOptions {

    @Description("Path of the nico zip file to read from gcs or local file path")
    @Required
    String getInputFile();
    void setInputFile(String value);

    @Description("dataset.table_name in your bigquey")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  static TableSchema getTableSchema(){
    final List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("video_id").setType("STRING"));
    fields.add(new TableFieldSchema().setName("created_at").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("vpos").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("command").setType("STRING"));
    fields.add(new TableFieldSchema().setName("content").setType("STRING"));
    fields.add(new TableFieldSchema().setName("easy").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("owner").setType("INTEGER"));
    return new TableSchema().setFields(fields);
  }

  static void runNicoLoad(NicoLoadOptions options) {
    Logger LOG = LoggerFactory.getLogger(NicoCommentLoad.class);
    LOG.info("input="+options.getInputFile()+"\toutput="+options.getOutput());
    Pipeline p = Pipeline.create(options);
      PCollection<TableRow> tRows = p.apply(FileIO.match().filepattern(options.getInputFile()))
        .apply(FileIO.readMatches().withCompression(Compression.ZIP))
        .apply(new ZipToTableRow());
      TableSchema schema = getTableSchema();
      tRows.apply(
          BigQueryIO.writeTableRows()
                  .withSchema(schema)
                  .to(options.getOutput())
                  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                  .withTimePartitioning(new TimePartitioning().setField("created_at").setType("MONTH")));
    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    NicoLoadOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(NicoLoadOptions.class);
    runNicoLoad(options);
  }
}
