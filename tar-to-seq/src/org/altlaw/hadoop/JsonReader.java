/* TarToSeqFile.java - Convert tar files into Hadoop SequenceFiles.
 *
 * Copyright (C) 2008 Stuart Sierra
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http:www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.altlaw.hadoop;

/* From ant.jar, http://ant.apache.org/ */
import org.apache.tools.bzip2.CBZip2InputStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

/* From hadoop-*-core.jar, http://hadoop.apache.org/
 * Developed with Hadoop 0.16.3. */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
//import java.io.File;
//import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/** Utility to convert tar files into Hadoop SequenceFiles.  The tar
 * files may be compressed with GZip or BZip2.  The output
 * SequenceFile will be stored with BLOCK compression.  Each key (a
 * Text) in the SequenceFile is the name of the file in the tar
 * archive, and its value (a BytesWritable) is the contents of the
 * file.
 *
 * <p>This class can be run at the command line; run without
 * arguments to get usage instructions.
 *
 * @author Stuart Sierra (mail@stuartsierra.com)
 * @see <a href="http://hadoop.apache.org/core/docs/r0.16.3/api/org/apache/hadoop/io/SequenceFile.html">SequenceFile</a>
 * @see <a href="http://hadoop.apache.org/core/docs/r0.16.3/api/org/apache/hadoop/io/Text.html">Text</a>
 * @see <a href="http://hadoop.apache.org/core/docs/r0.16.3/api/org/apache/hadoop/io/BytesWritable.html">BytesWritable</a>
 */
public class JsonReader {
    public JsonReader(){

    }

    private String getSMID(String stm){
	String[] basenames=stm.split("/",-1);
	int sz=basenames.length;
	String basename=basenames[sz-1];
	String filename=basename.split("\\.",-1)[0];
	return filename;
    }
    private void  appendStringToOuput(String k,String v,SequenceFile.Writer output) throws IOException{
	Text key = new Text(k);
	BytesWritable value = new BytesWritable(v.getBytes());
	output.append(key,value);
    }
    public  void addSmidInJson(String src,String filename,SequenceFile.Writer output) throws Exception {
	InputStreamReader isr = new InputStreamReader(new FileInputStream(src));
	BufferedReader br = new BufferedReader(isr);
	String line;
	StringBuffer strings = new StringBuffer();
	while((line= br.readLine()) != null){
	    JSONObject obj=(JSONObject)JSONValue.parse(line);
	    obj.put("filename",filename);
	    obj.put("video_id",this.getSMID(filename));
	    StringWriter out = new StringWriter();
	    obj.writeJSONString(out);
	    String jsonText = out.toString();
	    this.appendStringToOuput(filename,jsonText,output);
	}
	br.close();
	isr.close();
    }
}
