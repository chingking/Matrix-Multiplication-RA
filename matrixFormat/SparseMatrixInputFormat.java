package matrixFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.LineReader;

public class SparseMatrixInputFormat extends FileInputFormat<IntArrayWritable, DoubleArrayWritable>
{
	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	private static final double SPLIT_SLOP = 1.1;   // 10% slop
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files"; // Normally, it's 2
	
	public RecordReader<IntArrayWritable, DoubleArrayWritable> createRecordReader(InputSplit input, TaskAttemptContext tac) throws IOException 
	{
		tac.setStatus(input.toString());
		return new MatrixRecordReader();
	}
	/** 
	   * Logically splits the set of input files for the job, splits N lines
	   * of the input as one split.
	   * 
	   * @see FileInputFormat#getSplits(JobContext)
	   */
	  public List<InputSplit> getSplits(JobContext job)	  throws IOException 
	  {
	    List<InputSplit> splits = new ArrayList<InputSplit>();
	    List<FileStatus> files = listStatus(job);
	    //Get the paths of two matrixes 
	    Path p1 = files.get(0).getPath();
	    Path p2 = files.get(1).getPath();
	    
	    Configuration conf = job.getConfiguration();
	    FileSystem  fs1 = p1.getFileSystem(conf);
	    FileSystem  fs2 = p2.getFileSystem(conf);
	    
	    LineReader lr = null, lr2 = null;
	    try {
	      FSDataInputStream in1  = fs1.open(p1);
	      FSDataInputStream in2  = fs2.open(p2);
	      lr = new LineReader(in1, conf);
	      lr2 = new LineReader(in2, conf);
	      Text tmpText = new Text();
	      //int numPartBlks = 0;
	      int rowLength = conf.getInt("rowLen",0);
	      int colLength = conf.getInt("colLen",0);
	      int colBLength = conf.getInt("colBLen",0);
	      int blkRow = conf.getInt("blkRow",0);
	      int blkCol = conf.getInt("blkCol",0);
	      int blkBCol = conf.getInt("blkBCol",0);
	      int nSlot = conf.getInt("nSlot", 1);
	      int method = (conf.get("method").compareTo("IPB")==0)?1:((conf.get("method").compareTo("OPB")==0)?2:0);; // 0: naive, 1: IPB, 2: OPB
	      // For the first matrix
	      long begin=0;
	      long length=0;
	      // For the second matrix
	      long begin2=0;
	      long length2=0;

	      //int num = 0;
	      /* Read first matrix in column-wise, strip some columns for all rows */
	      //System.out.println("Read first matrix in column-wise, strip some columns for all rows");
	      int stripLen = blkCol;
	      int nslot=0;
	      int bytes = 0, bytes2 = 0;
	      int numLines =0;
	      
	      if (method == 2)
	      {
	    	//For OPB case
		      while ((bytes = lr.readLine(tmpText)) > 0 && (bytes2 = lr2.readLine(tmpText)) > 0) 
		      {
		    	  numLines++;
		    	  length += bytes;
		    	  length2 += bytes2;
		    	  if (bytes > conf.getInt("buffer.size.A", 0)) 
		        		conf.setInt("buffer.size.A", (int)bytes);
		        	if (bytes2 > conf.getInt("buffer.size.B", 0)) 
		        		conf.setInt("buffer.size.B", (int)bytes2);
		    	  if (numLines == (blkCol)) {
		        	splits.add(createFileSplit(nslot++, p1, begin, length, p2, begin2, length2));
		            begin += length;
		            begin2 += length2;
		            length = 0;
		            length2 = 0;
		            numLines = 0;
		          }
		      } 
	      }
	      else
	      {
	    	  //For IPB or naive, have to use whole matrix B
	    	  //Go through matrix B to know the total length
	    	  while ((bytes2 = lr2.readLine(tmpText)) > 0) 
		      {
		    	  length2 += bytes2;
		    	  if (bytes2 > conf.getInt("buffer.size.B", 0)) 
		        		conf.setInt("buffer.size.B", (int)bytes2);
		      } 
	    	  //Go through matrix A, block-by-block
	    	  while ((bytes = lr.readLine(tmpText)) > 0 ) 
		      {
		    	  numLines++;
		    	  length += bytes;
		    	  if (bytes > conf.getInt("buffer.size.A", 0)) 
		        		conf.setInt("buffer.size.A", (int)bytes);
		          if (numLines == (blkCol)) {
		        	splits.add(createFileSplit(nslot++, p1, begin, length, p2, begin2, length2));
		            begin += length;
		            length = 0;
		            numLines = 0;
		          }
		      } 
	      }
	      /* Read second matrix in row-wise, only get beginning position of each row, calculate block-by-block while running */
	      //System.out.println("Read second matrix in row-wise, strip some rows for all columns");
  	  
	    } catch(Exception e){}
	    finally {
	      if (lr != null) {
	        lr.close();
	      }
	    }
	    return splits; 
	  }
	  /**
	   * NLineInputFormat uses LineRecordReader, which always reads
	   * (and consumes) at least one character out of its upper split
	   * boundary. So to make sure that each mapper gets N lines, we
	   * move back the upper split limits of each split 
	   * by one character here.
	   * @param f1  Path of file
	   * @param begin  the position of the first byte in the file to process
	   * @param length  number of bytes in InputSplit
	   * @return  FileSplit
	   */
	  protected static MatrixFileSplit createFileSplit(int id, Path f1, long begin, long length, Path f2, long begin2, long length2) {
		    return new MatrixFileSplit(id, f1,f2, begin, begin2, length, length2, new String[] {}, new String[] {});
		  }
}
