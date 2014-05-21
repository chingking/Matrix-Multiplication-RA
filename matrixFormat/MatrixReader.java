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

package matrixFormat;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * A class that provides a line reader from an input stream.
 * Depending on the constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR),
 * or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated
 * line.
 */
public class MatrixReader {

  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private int bufSizeA=bufferSize, bufSizeB=bufferSize;
  //private FileSystem fs1, fs2;
  private Path p1, p2;
  private Configuration conf;
  private FSDataInputStream in;
  private FSDataInputStream in2;
  private LineReader lr=null, lr2=null;
  //private BufferedReader pos1;
  //private BufferedReader pos2;
  private byte[] buffer;
  private byte[] buffer2;
  private long start[];
  private long start2[];
  private long initStart2[];
  // the number of bytes of real data in the buffer
  private int bufferLength = 0;
  private int bufferLength2 = 0;
  // the current position in the buffer
  private int bufferPosn = 0;
  private int bufferPosn2 = 0;
  
  private int  bytesConsumed = 0, bytesConsumed2 = 0;
  /*Assume two input matix are identical, so share the same position related variables*/

  //private int readMethod = 0; // Matrix type: 0->Line by line, 1->Block by Block
  
  private static final byte CR = '\r';
  private static final byte LF = '\n';
  private static final byte SPACE = ' ';
  
  private static int blkId;
  private static int curAId = 0, curAIdC=0;
  private static int curBId = 0, curBIdC=0;
  private int rowLength;
  private int colLength;
  private int colBLength;
  private int blkRow;
  private int blkCol;
  private int blkBCol;
  private int nSlot;
  private String method="naive";
  private int naiveRA = 1; // For naive method only

  // The line delimiter
  private final byte[] recordDelimiterBytes;
  
  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public MatrixReader(FSDataInputStream in, FSDataInputStream in2) {
    this(in, in2, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the 
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
  public MatrixReader(FSDataInputStream in, FSDataInputStream in2, int bufferSize) {
    this.in = in;
    this.in2 = in2;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufSizeA];
    this.buffer2 = new byte[this.bufSizeB];
    this.recordDelimiterBytes = null;
  }
  public MatrixReader(Path in,Path in2, Configuration conf, long s1[], long s2[]) throws IOException {
	    this(in.getFileSystem(conf).open(in), in2.getFileSystem(conf).open(in2), conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
	    this.conf = conf;
	    this.p1=in;
	    this.p2=in2;
	    start=s1;
	    start2=s2;
	    initStart2 = Arrays.copyOf(s2, s2.length);
	    rowLength = conf.getInt("rowLen",0);
	    colLength = conf.getInt("colLen",0);
	    blkRow = conf.getInt("blkRow",0);
	    blkCol = conf.getInt("blkCol",0);
	    blkBCol = conf.getInt("blkBCol",0);
	    
	    nSlot = conf.getInt("nSlot", 1);
	    method = conf.get("method");
	    naiveRA = conf.getInt("naiveRA", 1);
	    
	    bufSizeA = conf.getInt("buffer.size.A", bufferSize);
	    bufSizeB = conf.getInt("buffer.size.B", bufferSize);
	}
  public MatrixReader(FSDataInputStream in,FSDataInputStream in2, Configuration conf, Path pos1, Path pos2) throws IOException 
  {
	    this(in, in2, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));

	    rowLength = conf.getInt("rowLen",0);
	    colLength = conf.getInt("colLen",0);
	    blkRow = conf.getInt("blkRow",0);
	    blkCol = conf.getInt("blkCol",0);
	    blkBCol = conf.getInt("blkBCol",0);
	    nSlot = conf.getInt("nSlot", 1);
	    method = conf.get("method");
	    naiveRA = conf.getInt("naiveRA", 1);
	    
	    bufSizeA = conf.getInt("buffer.size.A", bufferSize);
	    bufSizeB = conf.getInt("buffer.size.B", bufferSize);
	    
	    /*FileSystem fs = pos1.getFileSystem(conf);
	    Path pt[] = DistributedCache.getLocalCacheFiles(conf);
	    
	    this.pos1 = new BufferedReader(new FileReader(pt[0].toString()));
	    this.pos1 = new BufferedReader(new InputStreamReader(fs.open(pos1)));
	    this.pos2 = new BufferedReader(new InputStreamReader(fs.open(pos2)));
	    
	    String line = this.pos2.readLine();
	    String col2[] = line.split(" ");
	    this.start = new long[blkCol];
	    this.start2 = new long[blkCol];
	    this.initStart2 = new long[blkCol];
	    
	    for (int i=0 ; i<blkCol ; i++)
	    	this.initStart2[i] = Integer.parseInt(col2[i]);
	    getNewPosn();*/
	    
	}
  	/*public void getNewPosn() throws IOException 
  	{
  		String line = new String();
	    line = this.pos1.readLine();
	    String col[] = line.split(" ");
	    
	    for (int i=0 ; i<blkCol ; i++)
	    {
	    	this.start[i] = Integer.parseInt(col[i]);
	    }
	    System.arraycopy(initStart2, 0, start2, 0, initStart2.length);
	   // System.out.println("MatrixReader(): pos A, "+line);
  	}*/
  
  public MatrixReader(FSDataInputStream in,FSDataInputStream in2, Configuration conf, long s1[], long s2[]) throws IOException {
    this(in, in2, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));

	//LOG.info("MatrixReader.construbtor(): with available memory "+Runtime.getRuntime().freeMemory()+" in "+Runtime.getRuntime().totalMemory()+" and "+Runtime.getRuntime().maxMemory());
    start=s1;
    start2=s2;
    
    rowLength = conf.getInt("rowLen",0);
    colLength = conf.getInt("colLen",0);
    blkRow = conf.getInt("blkRow",0);
    blkCol = conf.getInt("blkCol",0);
    blkBCol = conf.getInt("blkBCol",0);
    colBLength = conf.getInt("colBLen",0);
    nSlot = conf.getInt("nSlot", 1);
    method = conf.get("method");
    if (method.compareTo("OPB")==0)
    	initStart2 = Arrays.copyOf(s2, s2.length);
    else
    	initStart2 = Arrays.copyOf(s1, s1.length);
    naiveRA = conf.getInt("naiveRA", 1);
    
    bufSizeA = conf.getInt("buffer.size.A", bufferSize);
    bufSizeB = conf.getInt("buffer.size.B", bufferSize);
    /*localFilename = "file:///tmp/tmp_B_"+conf.get("mapred.task.id");
    localPath = new Path(localFilename);
    localFS = localPath.getFileSystem(conf);
    fwB = new BufferedWriter(new OutputStreamWriter(localFS.create(localPath)));*/
  }
  
  public MatrixReader(FSDataInputStream in,FSDataInputStream in2, Configuration conf, long s1, long s2) throws IOException {
	    this(in, in2, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));

		//LOG.info("MatrixReader.construbtor(): with available memory "+Runtime.getRuntime().freeMemory()+" in "+Runtime.getRuntime().totalMemory()+" and "+Runtime.getRuntime().maxMemory());
	    start=new long[1];
	    start[0]=s1;
	    start2=new long[1];
	    start2[0]=s2;
	    
	    rowLength = conf.getInt("rowLen",0);
	    colLength = conf.getInt("colLen",0);
	    colBLength = conf.getInt("colBLen",0);
	    blkRow = conf.getInt("blkRow",0);
	    blkCol = conf.getInt("blkCol",0);
	    blkBCol = conf.getInt("blkBCol",0);
	    nSlot = conf.getInt("nSlot", 1);
	    method = conf.get("method");
	    
	    lr = new LineReader(in);
	    lr2 = new LineReader(in2);
	    bufSizeA = conf.getInt("buffer.size.A", bufferSize);
	    bufSizeB = conf.getInt("buffer.size.B", bufferSize);
	  }
  
  public MatrixReader(FSDataInputStream in, FSDataInputStream in2, byte[] recordDelimiterBytes) {
    this.in = in;
    this.in2 = in2;
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.buffer = new byte[this.bufferSize];
    this.buffer2 = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }

  /**
   * Create a line reader that reads from the given stream using the
   * given buffer-size, and using a custom delimiter of array of
   * bytes.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public MatrixReader(FSDataInputStream in,FSDataInputStream in2, int bufferSize,
      byte[] recordDelimiterBytes) {
    this.in = in;
    this.in2 = in2;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
    this.buffer2 = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>, and using a custom delimiter of array of
   * bytes.
   * @param in input stream
   * @param conf configuration
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public MatrixReader(FSDataInputStream in, FSDataInputStream in2,Configuration conf,
      byte[] recordDelimiterBytes) throws IOException {
    this.in = in;
    this.in2 =in2;
    this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
    this.buffer = new byte[this.bufferSize];
    this.buffer2 = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }
  
  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    if (in != null)
	  in.close();
    if (in2 != null)
    	in2.close();

    //System.out.println("MatrixReader close(): Buffer size, "+this.bufSizeA+", "+this.bufSizeB);
    //System.out.println("MatrixReader close(): Total consumed bytes, "+bytesConsumed+" from A, "+bytesConsumed2+" from B");
    // if (start != null)
    //{
    //	System.out.println("MatrixReader.close(): Read time "+readTimeA+" ms for A, and start from pos "+this.start[0]+" to "+this.start[start.length-1]);
    //	System.out.println("MatrixReader.close(): Read time "+readTimeB+" ms for B, and start from pos "+this.initStart2[0]+" to "+this.initStart2[blkCol-1]);
    //}
  }
  
  	public int getBytesComsumed(int index)
  	{
  		return ((index == 0)?bytesConsumed:bytesConsumed2);
  	}
	
	/*
	private long posAc=0, posAr=0, posBc=0, posBr=0;	//Block id for two matrixes
	private Map<String, DoubleArrayWritable> blockA = new HashMap<String, DoubleArrayWritable>(); 	//Store the blocks divided from matrix A, <block ID, small matrix>
	private Map<String, DoubleArrayWritable> blockB = new HashMap<String, DoubleArrayWritable>(); 	//Store the blocks divided from matrix B, <block ID, small matrix>
	
	public boolean isMoreBlock()
	{
		return (blockA.containsKey(posAr+" "+posAc));
	}
	
	private int curRow=0;*/
  	private static final Log LOG = LogFactory.getLog(MatrixReader.class);
	// It consumes about 2.5 MB for a 500*500 matrix
	private DoubleArrayWritable curAVal=new DoubleArrayWritable();
	//private DoubleArrayWritable tmpB = new DoubleArrayWritable();
	/*private BufferedWriter fwB;
	private BufferedReader brB;
	private String localFilename;
	private FileSystem localFS;
	private Path localPath ;
	private FSDataInputStream tmpFSIS;
	private boolean isReadyLocalB = false;*/

	public int getIPBlock(LongArrayWritable key, DoubleArrayWritable value) throws IOException
	{
		key.clear();
		value.clear(blkRow*blkCol*2);
		key.set(String.format("%09d", start[curAId*blkRow])+" "+curBIdC);
		if (curBIdC >= (int)Math.ceil((double)rowLength/blkRow))
			return 0;
		//Fetch first block from A
		if (in == null)
			in = p1.getFileSystem(this.conf).open(p1);
		for (int i=0; i<blkRow ; i++)
		{
			in.seek(start[i]);
			start[i]+=readElements(0, value, blkCol, true);
		}		
		//Fetch second block from B
		if (in2 == null)
			in2 = p2.getFileSystem(this.conf).open(p2);
		for (int i=0; i<blkCol ; i++)
		{
			in2.seek(start2[curBId*blkCol+i]);
			start2[curBId*blkCol+i]+=readElements(1, value, blkRow, true);
		}
		curAIdC++;
		curBId++;
		if (curBId == (int)Math.ceil((double)colLength/blkCol))
		{
			//curAId=0;
			curAIdC=0;
			curBId=0;
			curBIdC++;
			System.arraycopy(initStart2, 0, start, 0, initStart2.length);
		}
		return 1;
	}
	private long readTimeA=0, readTimeB=0, ST;
	private long readByteA=0, readByteB=0;
	//private DoubleArrayWritable tmpB2 = new DoubleArrayWritable();
	
	public boolean ensureMemory()
	{
		//System.out.println("ensureMemory: free "+Runtime.getRuntime().freeMemory()+", total "+Runtime.getRuntime().totalMemory()+", max "+Runtime.getRuntime().maxMemory());
		//System.out.println("to be used: "+(this.bufSizeA+this.bufSizeB));
		return Runtime.getRuntime().freeMemory() > (this.bufSizeA+this.bufSizeB)*2;
	}
	
	public int getOPBlock(LongArrayWritable key, DoubleArrayWritable value) throws IOException 
	{
		ST=System.currentTimeMillis();
		//LOG.info("MatrixReader(): Starting fetch a block");
		//adaptBlockSize();
		key.clear();
		key.set(curAId+" "+curBId);
		//System.out.println("Naive RA: "+naiveRA+"rows");
		if (curAId ==  (int)Math.ceil((double)rowLength/blkRow))
			return 0;
		//long length;
		if (curAVal.length()==0)
		{
			value.clear(blkRow*blkCol+blkCol*blkBCol);
			//int n=curVal.length();
			if (in == null)
				in = p1.getFileSystem(this.conf).open(p1);
			//Fetch first block from A
			for (int i=0; i<blkRow && curAId*blkRow+i < rowLength  ; i++)
			{
				in.seek(start[curAId*blkRow+i]);
				readElements(0, value, blkCol, true);
				//if ((value.length()-n) != blkCol)
					//System.out.println("getBlock: got "+(value.length()-n)+" elements on "+i);
				//n=value.length();
			}
			if (value.length() < blkRow*blkCol)
				value.add(blkRow*blkCol - value.length(), 0);
			//in.close();
			curAVal.set(value);
		}
		else
		{
			//value.set(curAVal);
			value.setPos(curAVal.length());
			this.readByteA=0;
		}
		if (in2 == null)
			in2 = p2.getFileSystem(this.conf).open(p2);
		//System.out.println("getBlock: after first round "+value.length()+", the length of copy "+curAVal.length());
		//LOG.info("MatrixReader(): Got half block");
		readTimeA += System.currentTimeMillis()-ST;
		System.out.println("MatrixReader: "+(System.currentTimeMillis()-ST)+" ms on read A for "+this.readByteA+" bytes");
		ST=System.currentTimeMillis();
		//Fetch second block from B
		for (int i=0; i<blkCol ; i++)
		{
			in2.seek(start2[i]);
			//System.out.println("MatrixReader: Read B begin "+start2[i]);
			start2[i]+=readElements(1, value, blkBCol, true);
		}			
		if (value.length() < blkRow*blkCol+blkCol*blkBCol)
			value.add(blkRow*blkCol+blkCol*blkBCol - value.length(), 0);
		readTimeB += System.currentTimeMillis()-ST;
		System.out.println("MatrixReader: "+(System.currentTimeMillis()-ST)+" ms on read B for "+this.readByteB+" bytes");
		//in2.close();
		curBId++;
		if (curBId == (int)Math.ceil((double)rowLength/blkBCol))
		{
			curBId=0;
			curAId++;
			curAVal.clear();
			System.arraycopy(initStart2, 0, start2, 0, initStart2.length);
		}
		//LOG.info("MatrixReader(): Return block");
		//System.out.println("getBlock: after second round, "+value.length());
		return 1;
	}
	public int getSpareOPBlock(LongArrayWritable key, DoubleArrayWritable value) throws IOException 
	{
		if (curAId >= blkCol)
			return 0;
		key.clear();
		value.clear();
		Text tmpVal = new Text();
		/*int oldA=curAId;
		do
		{*/
			lr.readLine(tmpVal);
			value.add(tmpVal.toString());
			//System.out.println("getSpareOPBlock: From "+start[0]+" A ("+tmpVal.getLength()+"):"+tmpVal.toString());
			tmpVal.clear();
			key.add(value.length()); // used to represent the boundary between vectors in a iteration
			lr2.readLine(tmpVal);
			//System.out.println("getSpareOPBlock: From "+start2[0]+" B ("+tmpVal.getLength()+"):"+tmpVal.toString());
			value.add(tmpVal.toString());
			//key.add(value.length()); // used to represent the boundary between iterations 
			curAId++;
		//} while (ensureMemory() && curAId < blkCol);
		//System.out.println("getSpareOPBlock: read "+(curAId-oldA)+" lines");
		return 1;
	}
	public int getSpareIPBlock(LongArrayWritable key, DoubleArrayWritable value) throws IOException 
	{
		if (curAId >= blkCol)
			return 0;
		key.clear();
		value.clear();
		Text tmpVal = new Text();
		lr.readLine(tmpVal);
		value.set(tmpVal.toString());
		tmpVal.clear();
		key.add(value.length()); // used to represent the boundary between two vector
		lr2.readLine(tmpVal);
		value.add(tmpVal.toString());
		curBId++;
		if (curBId >= blkBCol)
		{
			curBId=0;
			curAId++;
			in2.seek(0);
			lr2 = new LineReader(in2);
		}
		return 1;
	}
	/*For debug only, fetch the particular entry*/
	public String printElement(int index, long posn) throws IOException
	{
		byte buf[] = new byte[10];
		if (index == 0 )
		{
			in.seek(posn);
			in.read(buf, 10, buf.length);
		}
		else
		{
			in2.seek(posn);
			in2.read(buf, 10, buf.length);
		}
		//System.out.println(new String(buf, 0, 10));
		return new String(buf, 0, 10);
	}
	/* Only read out files for get positions, not store any data into memory */
	public int readElements(int index, DoubleArrayWritable val, int maxElement, boolean store) throws IOException
	{
		int numElement = 0;
		int newlineLength = 0;
	    boolean prevCharCR = false; //true of prev char was CR
	    //boolean skipElement = false;
	    long bytesConsumed = 0;
	    int bufLen = (store)?0:((index==0)?bufferLength:bufferLength2);
	    int bufpos = (store)?0:((index==0)?bufferPosn:bufferPosn2);
		byte buf[] = ((index==0)?buffer:buffer2);
		if (store)
      	  buf = new byte[((index==0)?bufSizeA:bufSizeB)];
		
	    do {
	    		int startPosn = bufpos;  //starting from where we left off the last time
	    		int nElemPosn = bufpos;	 // Record the position of next element
		        if (bufpos >= bufLen) {
		        	  startPosn = nElemPosn = bufpos = 0;
			          if (prevCharCR)
			        	  ++bytesConsumed; //account for CR from previous read
			          int b=0;
			          if (index == 0)
			          {
			        	  if (store)
			        	  {
			        		  do
			        		  {
			        			  b = in.read(buf, bufLen, buf.length-bufLen);
			        			  bufLen += b;
			        		  }while(b>0 && bufLen < buf.length); //make sure read out enough entries if there are more
			        	  }
			        	  else
			        		  bufLen = in.read(buf);
			          }
			          else
			          {
			        	  if (store)
			        	  {
			        		  do
			        		  {
			        			  b =  in2.read(buf, bufLen, buf.length-bufLen);
			        			  bufLen += b;
			        		  }while(b>0 && bufLen < buf.length);
			        	  }
			        	  else
			        		  bufLen = in2.read(buf);
			          }
			          if (b==-1) //compensation of bufLen for the situation of reading EOF
			        	  bufLen++;
			          if (bufLen <= 0)
			          {
			        	  if (numElement != maxElement && store )
			        	  {
			        		  val.add(maxElement-numElement, 0);
			        		  numElement=maxElement;
			        	  }
			        	  //System.out.println("readElements(): read nothing, reach EOF: buf length "+buf.length+", val len"+val.length());
			        	  break; // EOF
			          }
		        }
		        //System.out.println("readElements(): read, buf max length "+buf.length+", real buf length "+bufLen);
		        //System.out.println("readElements(): buf contains: "+new String(buf, 0, buf.length));
		        for (; bufpos < bufLen; ++bufpos) { //search for newline
			          if (buf[bufpos] == LF) {
			        	newlineLength = (prevCharCR) ? 2 : 1;
			        	// Get the last entry if there is no additional space behind it
			        	if (buf[bufpos-newlineLength] != SPACE)
			        	{
			        		if (store)
				        		  val.add(buf, nElemPosn, bufpos-nElemPosn);	
				        	  //System.out.print(new String(buf, nElemPosn, bufpos-nElemPosn)+" ");
				        	  numElement++;
				        	  nElemPosn = bufpos+1;
			        	}
			        	//System.out.println("\r\nreadElements(): Reach LF, read "+numElement+" entries");
			            if (!store)
			            	numElement=maxElement;
			            if (numElement != maxElement)
			            {
			            	//System.out.println("readElements(): read a LF, last entry "+val.get(val.length()-1)+"ready to add zero, cur length "+val.length());
			            	val.add(maxElement-numElement, 0);
			            	numElement=maxElement;
			            	//System.out.println("readElements(): after adding, "+val.length());
			            }
			            ++bufpos; // at next invocation proceed from following byte
			            break;
			          }
			          if (prevCharCR) { //CR + notLF, we are at notLF
			            newlineLength = 1;
			            break;
			          }
			          prevCharCR = (buf[bufpos] == CR);
			          if (!prevCharCR && numElement==maxElement)
			        	  break;
			          if (buf[bufpos] == SPACE) 
			          {
			        	  if (store)
			        		  val.add(buf, nElemPosn, bufpos-nElemPosn);	
			        	  //System.out.print(new String(buf, nElemPosn, bufpos-nElemPosn)+" ");
			        	  numElement++;
			        	  nElemPosn = bufpos+1;
			          }
		        }
		        bytesConsumed += ((bufpos - startPosn));
	      } while (numElement != maxElement );
	      //System.out.println("\r\n"+bufpos+", "+bytesConsumed);
		  if (index == 0)
		  {
			  bufferPosn = bufpos;
			  bufferLength = bufLen;
			  buffer = buf;
			  this.readByteA = bytesConsumed;
			  this.bytesConsumed+=bytesConsumed;
		  }
		  else
		  {
			  bufferPosn2 = bufpos;
			  bufferLength2 = bufLen;
			  buffer2 = buf;
			  this.readByteB = bytesConsumed;
			  this.bytesConsumed2+=bytesConsumed;
		  }
		  if (bytesConsumed > (long)Integer.MAX_VALUE)
	    	  throw new IOException("Too many bytes before newline: " + bytesConsumed);
	      return (int)bytesConsumed;
	}
}
