
package matrixFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MatrixFileSplit extends InputSplit implements Writable //implements RawComparator<MatrixFileSplit>
{
	public class splittedBlocks
	{
		public Path file;
		public Path posFile;
		public long start[];
		public long length;
		public String[] hosts;
		public splittedBlocks(){}
		public splittedBlocks(Path p, Path pos, String[] hosts)
		{
			this.file = p;
			//this.length =len;
			this.start = new long[1];
			this.posFile = pos;
			this.hosts = hosts;
		}
		public splittedBlocks(Path p, long start[], long length, String[] hosts)
		{
			this.file = p;
			this.length =length;
			this.start = start;
			this.hosts = hosts;
		}
		public splittedBlocks(Path p, long start, long length, String[] hosts)
		{
			this.file = p;
			this.length =length;
			this.start= new long[1];
			this.start[0] = start;
			this.hosts = hosts;
		}
	}
	
	private splittedBlocks matA=new splittedBlocks(), matB = new splittedBlocks();
	private int blkID;
	
	public MatrixFileSplit(){}

	/*public MatrixFileSplit(Path f1, Path f2, long s1, long l1, long l2, String[] hosts1) {
		//super(f1, s1, l1, hosts1);
	    matA = new splittedBlocks(f1,s1,l1,hosts1,0);
		matB = new splittedBlocks(f2,0,l2,hosts1,0);
		//showAllInfo();
	}*/
	public MatrixFileSplit(int id, Path f1, Path pos1, Path f2, Path pos2,String[] hosts1, String[] hosts2) {
		//super(f1, s1, l1, hosts1);
		blkID = id;
	    matA = new splittedBlocks(f1,pos1,hosts1);
		matB = new splittedBlocks(f2,pos2,hosts2);
		//showAllInfo();
	}
	public MatrixFileSplit(int id, Path f1, Path f2, long s1[], long s2[],long l1,long l2, String[] hosts1, String[] hosts2) {
		//super(f1, s1, l1, hosts1);
		blkID = id;
	    matA = new splittedBlocks(f1,s1,l1, hosts1);
		matB = new splittedBlocks(f2,s2,l2, hosts2);

		//showAllInfo();
	}
	public MatrixFileSplit(int id, Path f1, Path f2, long s1, long s2,long l1,long l2, String[] hosts1, String[] hosts2) {
		//super(f1, s1, l1, hosts1);
		blkID = id;
	    matA = new splittedBlocks(f1,s1,l1, hosts1);
		matB = new splittedBlocks(f2,s2,l2, hosts2);
		//showAllInfo();
	}
	public int getId()
	{
		return blkID;
	}
	public long[] getStarts (int index)
	{
		return (index==0)?matA.start:matB.start;
	}
	public void showAllInfo() //For debug
	{
		//System.out.println("Show all information of MatrixFileSplit");
		System.out.println("MatrixFileSplit: Path1: "+matA.file.getName()+", start "+matA.start);
		System.out.println("MatrixFileSplit: Path2: "+matB.file.getName()+", start "+matB.start);
	}
	  /** The file containing this split's data. */
	public Path getPath() { return matA.file; }
	public Path getPath(int index) { return (index==0)?matA.file:matB.file; }
	
	  
	/** The position of the first byte in the file to process. */
	public long getStart() { return matA.start[0]; }
	public long getStart(int index) { return (index==0)?matA.start[0]:matB.start[0]; }
	public Path getStartPath(int index) { return (index==0)?matA.posFile:matB.posFile; }  
	  /** The number of bytes in the file to process. */
	//public long getLength1() { return matA.length; }
	public long getLength(int index) { return (index==0)?(matA.length):(matB.length); }

	//public String toString(int index) { return (index==0)?(matA.file + ":" + matA.start + "+" + matA.length):(matB.file + ":" + matB.start + "+" + matB.length); }

	  ////////////////////////////////////////////
	  // Writable methods
	  ////////////////////////////////////////////

	  public String[] getLocations(int index) throws IOException {
	    if (((index==0)?matA.hosts:matB.hosts) == null) {
	      return new String[]{};
	    } else {
	      return ((index==0)?matA.hosts:matB.hosts);
	    }
	  }

	@Override
	public void readFields(DataInput in) throws IOException {
		matA.file = new Path(Text.readString(in));
		//matA.posFile = new Path(Text.readString(in));
		int lenA = in.readInt();
		matA.start = new long[lenA];
		for (int i=0 ; i < lenA ; i++)
			matA.start[i] = in.readLong();
	    //matA.length = in.readLong();
	    matA.hosts = null;	
	    matB.file = new Path(Text.readString(in));
	    //matB.posFile = new Path(Text.readString(in));
	    int lenB = in.readInt();
	    matB.start = new long[lenB];
	    for (int i=0 ; i < lenB ; i++)
	    	matB.start[i] = in.readLong();
	    //matB.length = in.readLong();
	    matB.hosts = null;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, matA.file.toString());
		//Text.writeString(out, matA.posFile.toString());
		out.writeInt(matA.start.length);
		for (int i=0 ; i < matA.start.length ; i++)
			out.writeLong(matA.start[i]);
	    //out.writeLong(matA.length);
	    Text.writeString(out, matB.file.toString());
	    //Text.writeString(out, matB.posFile.toString());
	    out.writeInt(matB.start.length);
	    for (int i=0 ; i < matB.start.length ; i++)
			out.writeLong(matB.start[i]);
	    //out.writeLong(matB.length);
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return matA.length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
	    if (matA.hosts == null) {
	        return new String[]{};
	      } else {
	        return matA.hosts;
	      }
	}

}
