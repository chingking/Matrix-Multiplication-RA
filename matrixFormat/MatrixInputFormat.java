package matrixFormat;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.JobContext;

public class MatrixInputFormat extends FileInputFormat<LongArrayWritable, DoubleArrayWritable>
{
	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	private static final double SPLIT_SLOP = 1.1;   // 10% slop
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files"; // Normally, it's 2
	
	public RecordReader<LongArrayWritable, DoubleArrayWritable> createRecordReader(InputSplit input, TaskAttemptContext tac) throws IOException 
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
	    
	    MatrixReader mr = null;
	    try {
	      FSDataInputStream in1  = fs1.open(p1);
	      FSDataInputStream in2  = fs2.open(p2);
	      mr = new MatrixReader(in1, in2);
	      DoubleArrayWritable tmpMat = new DoubleArrayWritable();
	      //int numPartBlks = 0;
	      int rowLength = conf.getInt("rowLen",0);
	      int colLength = conf.getInt("colLen",0);
	      int blkRow = conf.getInt("blkRow",0);
	      int blkCol = conf.getInt("blkCol",0);
	      int blkBCol = conf.getInt("blkBCol",0);
	      int nSlot = conf.getInt("nSlot", 1);
	      int method = (conf.get("method").compareTo("IPB")==0)?1:((conf.get("method").compareTo("OPB")==0)?2:0);; // 0: naive, 1: IPB, 2: OPB
	      // For the first matrix
	      long begin [][]= new long[nSlot][rowLength];
	      long length []= new long[nSlot];
	      // For the second matrix
	      long begin2 [][]= new long[nSlot][rowLength];
	      long length2 []= new long[nSlot];
	      for (int i=0 ; i<nSlot ; i++)
	      {
	    	  begin[i] = new long[rowLength];
	    	  begin2[i] = new long[rowLength];
	      }
	      //int num = 0;
	      /* Read first matrix in column-wise, strip some columns for all rows */
	      //System.out.println("Read first matrix in column-wise, strip some columns for all rows");
          
	      int stripLen = blkCol;
	      long commLen=-1;
	      int bytes;
	      for (int i=0 ; i<rowLength ; i++)
	      {
	    	  for (int j=0 ; j<nSlot ; j++)
	    	  {
	    		  begin[j][i] = commLen+1;
	    		  bytes = mr.readElements(0, tmpMat, stripLen, false);
	    		  commLen += bytes;
	    		  length[j] += bytes;
	    		  if ( bytes > conf.getInt("buffer.size.A", 0))
			    		 conf.setInt("buffer.size.A", bytes);
	    	  }
	      }	      
	      
	      /* Read second matrix in row-wise, only get beginning position of each row, calculate block-by-block while running */
	      //System.out.println("Read second matrix in row-wise, strip some rows for all columns");
    	  commLen=-1;
    	  stripLen = colLength;
    	  for (int i=0 ; i<nSlot ; i++)
	      {
	    	  for (int j=0 ; j<blkCol ; j++)
	    	  {
	    		  begin2[i][j] = commLen+1;
    			  bytes = mr.readElements(1,tmpMat, stripLen, false);
		    	  commLen += bytes;
		    	  length2[i]+=bytes;
		    	  if ( bytes > conf.getInt("buffer.size.B", 0))
		    		  conf.setInt("buffer.size.B", bytes);
	    	  }
	      }    	  
	      for (int i=0 ; i<nSlot ; i++)
	      {
	    	  //System.out.println("MatrixInputFormat.getsplits(): mat A, "+i+" block: begin at "+begin[i]+", length "+length[i]);
	    	  //System.out.println("MatrixInputFormat.getsplits(): mat B, "+i+" block: begin at "+mr2.printElement(1, begin2[i][0])+", length "+length2[i]);
	    	  if (method==2)
	    		  splits.add(createFileSplit(i, p1, begin[i], length[i], p2, begin2[i], length2[i]));
	    	  else
	    		  splits.add(createFileSplit(i, p1, begin[i], length[i], p2, begin2[0], length2[i]));
	    	  //splits.add(createFileSplit(p1, posFile1, p2, posFile1));
	      }
	    } catch(Exception e){}
	    finally {
	      if (mr != null) {
	        mr.close();
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
	  protected static MatrixFileSplit createFileSplit(int id, Path f1, long begin[], long length, Path f2, long begin2[], long length2) {
	    return new MatrixFileSplit(id, f1,f2, begin, begin2, length, length2, new String[] {}, new String[] {});
	  }
	  protected static MatrixFileSplit createFileSplit(int id, Path f1, Path begin1, Path f2, Path begin2) {
		    return new MatrixFileSplit(id, f1,f2, begin1, begin2, new String[] {}, new String[] {});
	  }
}
