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
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;

import org.apache.tools.ant.DirectoryScanner;


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
public class TarToSeqFile {

    private File outputFile;
    private LocalSetup setup;
    private String inputDir;

    /** Sets up Configuration and LocalFileSystem instances for
     * Hadoop.  Throws Exception if they fail.  Does not load any
     * Hadoop XML configuration files, just sets the minimum
     * configuration necessary to use the local file system.
     */
    public TarToSeqFile() throws Exception {
        setup = new LocalSetup();
    }

    /** Sets the input tar file. */
    public void setInputDir(String str){
	this.inputDir=str;
    }

    /** Sets the output SequenceFile. */
    public void setOutput(File outputFile) {
        this.outputFile = outputFile;
    }

    /** Performs the conversion. */
    public void execute() throws Exception {
        SequenceFile.Writer output = null;
	String[] filelist=null;
        try {
            output = openOutputFile();
	    filelist = listFiles(this.inputDir);
	    for(String f:filelist){
		String src=this.inputDir+File.separator+f;
		JsonReader jr=new JsonReader();
		System.out.println("filename:"+src);
		jr.addSmidInJson(src,src,output);
	    }
        } finally {
            if (output != null) { output.close(); }
        }
    }
    private String[] listFiles(String dir){
	DirectoryScanner scanner = new DirectoryScanner();
	scanner.setIncludes(new String[]{"**/*.dat"});
	scanner.setBasedir(dir);
	scanner.setCaseSensitive(false);
	scanner.scan();
	return scanner.getIncludedFiles();
    }

    private SequenceFile.Writer openOutputFile() throws Exception {
        Path outputPath = new Path(outputFile.getAbsolutePath());
        return SequenceFile.createWriter(setup.getLocalFileSystem(), setup.getConf(),
                                         outputPath,
                                         Text.class, BytesWritable.class,
                                         SequenceFile.CompressionType.BLOCK);
    }


    /** Runs the converter at the command line. */
    public static void main(String[] args) {
        if (args.length != 2) {
            exitWithHelp();
        }

        try {
            TarToSeqFile me = new TarToSeqFile();
	    me.setInputDir(new String(args[0]));
            me.setOutput(new File(args[1]));
            me.execute();
        } catch (Exception e) {
            e.printStackTrace();
            exitWithHelp();
        }
    }

    public static void exitWithHelp() {
        System.err.println("Usage: java org.altlaw.hadoop.TarToSeqFile <dir> <output>\n\n");
        System.exit(1);
    }
}
