package matrixFormat;
/**
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.lib.input.*;

public class MatrixRecordReader extends RecordReader<Text, DoubleArrayWritable> {

	private static final Log LOG = LogFactory.getLog(MatrixRecordReader.class);
	private CompressionCodecFactory compressionCodecs = null;
	//We will have two matrix to read at the same time.
	private long start1;
	private long start2;
	private long pos1;
	private long pos2;
	private long end1;
	private long end2;
	private MatrixReader in;
	//private MatrixReader in2;
	private int maxLength;
	private int blkID = 0;
	private Text key = null;
	private DoubleArrayWritable value = null;
	private Seekable filePosition1;
	private Seekable filePosition2;
	private CompressionCodec codec;
	private Decompressor decompressor;
	private int method=0; // 0: naive, 1: IPB, 2: OPB
	private boolean sparse=false;

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    //FileSplit split = (FileSplit) genericSplit;

	MatrixFileSplit split = (MatrixFileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    method = (job.get("method").compareTo("IPB")==0)?1:((job.get("method").compareTo("OPB")==0)?2:0);
    sparse = job.getBoolean("Sparse", false);
    this.maxLength = job.getInt("mapred.matrixrecordreader.maxlength", Integer.MAX_VALUE);
    
    start1 = split.getStart();
    start2 = split.getStart(1);
    end1 = start1 + split.getLength(0);
    end2 = start2 + split.getLength(1);
    blkID = split.getId();
    final Path file = split.getPath(0);
    final Path file2 = split.getPath(1);
    compressionCodecs = new CompressionCodecFactory(job);
    codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FileSystem fs2 = file2.getFileSystem(job);
    FSDataInputStream fileIn1 = fs.open(split.getPath(0));
    FSDataInputStream fileIn2 = fs2.open(split.getPath(1));
    //FileInputStream fileIn2 = new FileInputStream(file2.toString()); 
    //Don't care the compression stuff
    /*if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn =
          ((SplittableCompressionCodec)codec).createInputStream(
            fileIn1, decompressor, start1, end1,
            SplittableCompressionCodec.READ_MODE.BYBLOCK);
        final SplitCompressionInputStream cIn2 =
                ((SplittableCompressionCodec)codec).createInputStream(
                  fileIn2, decompressor, start2, end2,
                  SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = new MatrixReader(cIn, cIn2);
        start1 = cIn.getAdjustedStart();
        end1 = cIn.getAdjustedEnd();
        filePosition1 = cIn;
      } else {
        in = new MatrixReader(codec.createInputStream(fileIn1, decompressor), codec.createInputStream(fileIn2, decompressor), job, split.getStarts(0), split.getStarts(1) );
        filePosition1 = fileIn1;
      }
    } else {*/
      fileIn1.seek(start1);
      fileIn2.seek(start2);
      if (sparse)
      {
    	  in = new MatrixReader(fileIn1, fileIn2, job, split.getStart(0), split.getStart(1));
      }
      else
      {
    	  in = new MatrixReader(fileIn1, fileIn2, job, split.getStarts(0), split.getStarts(1));
      }
      
      //in = new MatrixReader(file, file2, job, split.getStarts(0), split.getStarts(1));
      filePosition1 = fileIn1;
      filePosition2 = fileIn2;
    //}
    
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    /*if (start1 != 0) {
    	start1 += in.readOldBlock(maxLength, maxBytesToConsume(pos1));
    	this.pos1 = start1;
    }

    in.readBlocks(maxLength, maxBytesToConsume(pos1));
    start1 += in.getBytesComsumed(0);
    //start2 += in.getBytesComsumed(1);
    this.pos1 = start1;*/
  }
  
  private boolean isCompressedInput() {
    return (codec != null);
  }

  private int maxBytesToConsume(long pos) {
    return isCompressedInput()
      ? Integer.MAX_VALUE
      : (int) Math.min(Integer.MAX_VALUE, end1 - pos);
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition1) {
      retVal = filePosition1.getPos();
    } else {
      retVal = pos1;
    }
    return retVal;
  }

  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new Text();
    }
    
    if (value == null) {
      value = new DoubleArrayWritable();
    }
    int isValue = 0;
    if (sparse)
    	isValue = (method==2)?in.getSpareOPBlock(key, value):in.getSpareIPBlock(key, value);
    else
    	isValue = (method==2)?in.getOPBlock(key, value):in.getIPBlock(key, value);
    	
    if (isValue==1)
    {
    	//Put the block ID in the key
    	//key.set(blkID+" "+key.toString());
        return true;
    }
    else
    {
    	key = null;
    	value = null;
    	return false;
    }
  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public DoubleArrayWritable getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start1 == end1) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
        (getFilePosition() - start1) / (float)(end1 - start1));
    }
  }

  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }
}
