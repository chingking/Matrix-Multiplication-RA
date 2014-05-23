
import java.io.*;
import java.lang.instrument.Instrumentation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


import matrixFormat.*;

/*
 * Native matrix multiplication,
 *
 * King, Ching-Hsiang Chu
 * Started from 2013-10-08
 *
 */

public class nativeMatMult extends Configured implements Tool 
{
	private static volatile Instrumentation globalInstr;
	  public static void premain(String args, Instrumentation inst) {
	    globalInstr = inst;
	  }
	  public static long getObjectSize(Object obj) {
	    if (globalInstr == null)
	      throw new IllegalStateException("Agent not initted");
	    return globalInstr.getObjectSize(obj);
	  }
	public static enum MapTimeCounters {
	        MAP_COMPUTATION_TIME,
	        MAP_IO_TIME 
	}  
	public static class Map extends Mapper<IntArrayWritable, DoubleArrayWritable, IntArrayWritable, DoubleArrayWritable>
	{
		private static final Log LOG = LogFactory.getLog(Map.class);
		private int blkRow, blkCol, blkBCol;
		private boolean caching = false;
		private int cnt=1;
		private int entryCount=0;
		Runtime rt = Runtime.getRuntime();
		DoubleArrayWritable output = new DoubleArrayWritable();
		long start=0, totalComp=0, totalIO=0;
		public void run(Context context) throws IOException, InterruptedException 
		{			
			LOG.info("Mapper.run(): Starting Mapper with "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory());
		    setup(context);
		    try {
			      while (context.nextKeyValue()) {
			        map(context.getCurrentKey(), context.getCurrentValue(), context);
			      }
		    } finally {
		    	context.getCounter(MapTimeCounters.MAP_COMPUTATION_TIME).increment(totalComp);
		    	context.getCounter(MapTimeCounters.MAP_IO_TIME).increment(totalIO); 
		    	//System.out.println("Mapper.run(): Closing, took "+(totalComp)+" ms on computation; "+(totalIO)+" ms on I/O process of HDFS or Local FS");
		    	cleanup(context);
		    }
		    //LOG.info("Mapper.run(): Finished");
		}
		protected void setup(Context context)
		{
			blkRow = context.getConfiguration().getInt("blkRow",0);
			blkCol = context.getConfiguration().getInt("blkCol",0);
			blkBCol = context.getConfiguration().getInt("blkBCol",0);
			System.out.println("map(): R*C*C = "+blkRow+" * "+blkCol+" * "+blkBCol);
			if (context.getConfiguration().get("method").compareTo("naive") == 0)
			{
				caching=true;	
				output.clear(blkRow*blkCol);
			}
		}
		
		// Map is responsible for multiplying two small matrix
		public void map(IntArrayWritable key, DoubleArrayWritable value, Context context) throws IOException, InterruptedException
		{
			System.out.println("map(): R*C = "+blkRow+" * "+blkCol+" with key "+key.toString()+", value length "+value.length());
			value.printMatrix(blkRow,blkCol,blkBCol);
			//blkRow = (value.length()/2)/blkCol;
			//System.out.println("map(): value "+value.toString());
			//LOG.info("Mapper(): Starting processing "+(cnt++)+" th record with key "+key.toString()+", value length "+value.length());
			//LOG.info("Mapper.map(): with available memory "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory()+". Block size: "+blkRow+"*"+blkCol);
			if (start > 0)
				totalIO += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();
			if (caching)
			{
				//output.add(value.multiply(blkRow,blkCol));
				totalComp += System.currentTimeMillis() - start;
				start = System.currentTimeMillis();
				//if (output.length() == blkCol)
				//{
					context.write(key, output);
					//System.out.println("map(): output key "+key.toString()+", value "+output.toString());
					//output.clear(blkRow*blkCol);
				//}
			}
			else
			{
				context.write(key, value.multiply(blkRow,blkCol,blkBCol));		
				totalComp += System.currentTimeMillis() - start;
				System.out.println("Mapper.run(): "+(System.currentTimeMillis() - start)+" ms on computation\n");
				start = System.currentTimeMillis();
			}
			//LOG.info("Mapper(): Finished");
		}
	}
	
	public static class Reduce extends Reducer<IntArrayWritable, DoubleArrayWritable, NullWritable, DoubleArrayWritable> 
	{
		private static final Log LOG = LogFactory.getLog(Reduce.class);
		private DoubleArrayWritable out = new DoubleArrayWritable(), pout = new DoubleArrayWritable();
		private int blkBCol;
		private boolean doSum = false;
		protected void setup(Context context)
		{
		//	blkRow = context.getConfiguration().getInt("blkRow",0);
			blkBCol = context.getConfiguration().getInt("blkBCol",0);
			Runtime rt = Runtime.getRuntime();
			LOG.info("Reduce.run(): Starting Reduce with "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory());
			if (context.getConfiguration().get("method").compareTo("OPB") == 0 )
				doSum = true;
			//blkCol = context.getConfiguration().getInt("blkCol",0);
		}
		protected void reduce(IntArrayWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException
		{
			//Runtime rt = Runtime.getRuntime();
			//LOG.info("Reduce.run(): Starting Reduce with "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory());
			//System.out.println("reduce(): R*C = "+blkRow+" * "+blkRow+", key "+key.toString());
			if (doSum)
			{
				for (DoubleArrayWritable val : values) 
				{
					//System.out.println("Reduce: val "+val.toString());
					out.sum(val);
				}
				for (int i=0 ; i<out.length(); i+=blkBCol)
				{
					pout.set(out.get(i, blkBCol));
					//System.out.println("Reduce(): output a record with length "+pout.length()+", "+pout.toString());
					context.write(NullWritable.get(), pout);
				}
			}
			else
			{
				for (DoubleArrayWritable val : values) 
				{
					//System.out.println("Reduce: val "+val.toString());
					//context.write(NullWritable.get(), val);
					for (int i=0 ; i<val.length(); i+=blkBCol)
					{
						pout.set(val.get(i, blkBCol));
						//System.out.println("Reduce(): output a record with length "+pout.length()+", "+pout.toString());
						context.write(NullWritable.get(), pout);
					}
				}
			}
		}
	}
	public static void main(String[] args) throws Exception
	{
		if (args.length < 5)
		{
			System.out.println("hadoop jar nativeMatMult.jar nativeMatMult <Matrix A> <Matrix B> <Output directory> <Row length of A> <Column/Row length of A/B> <Column length of B> <Sparsity of A> <Sparsity of B> <Method: naive, IPB, OPB> <Memory> <# of slots>");
			return;
		}
		int res=0;
		//Do it multiple times to get average result
		for (int i=0 ; i<1; i++)
		{
			long ptime, start = System.currentTimeMillis();
			// Transform Sparse matrix first
			if ( Integer.parseInt(args[6]) > 0)
			{
				String []newArgs = args;
				matrixTransform mt = new matrixTransform();
				String path[]={args[8], args[0], args[1]}; //First entry is <Method: IPB or OPB>, following two are path of matrices
				mt.run(path);
				if (args[8].compareTo("OPB") == 0)
				{
					newArgs[0] += "_sparse/CSC-r-00000";
					newArgs[1] += "_sparse/CSR-r-00000";
				}
				else
				{
					newArgs[0] += "_sparse/CSR-r-00000";
					newArgs[1] += "_sparse/CSC-r-00000";
				}
				//System.out.print("Arguments: ");
				//for (int j=0 ; j<newArgs.length ; j++)
					//System.out.print(newArgs[j]+" ");
				sparseMatMult smm = new sparseMatMult();
				res = smm.run(newArgs);
			}
			else
				res = ToolRunner.run(new Configuration(), new nativeMatMult(), args);
			ptime = System.currentTimeMillis() - start;
			System.out.print((ptime/1000)+" ");
		}
		System.out.println(" ");
		System.exit(res);
	}
	/*args: 0<Matrix A> 1<Matrix B> 2<Output directory> 3<Row length of A> 4<Column/Row length of A/B> 5<Column length of B> 
	 *      6<Sparsity of A> 7<Sparsity of B> 8<Method: naive, IPB, OPB> 9<Memory> 10<# of nodes>");
	*/
	
	/*Fod dense matrix multiplication, By default*/
	public int run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		
		int rowLen = Integer.parseInt(args[3]), colLen=Integer.parseInt(args[4]), colBLen=Integer.parseInt(args[5]);
		int sparseA = Integer.parseInt(args[6]), sparseB = Integer.parseInt(args[7]); // In fact, no need to know
		String method = args[8];
		int mem = Integer.parseInt(args[9]);
		int nNode = Integer.parseInt(args[10]);
		
		int slots = conf.getInt("mapred.tasktracker.map.tasks.maximum",18)*nNode;
		boolean RAE = true; //Enable Resource-aware Enhancement strategy or not
		int blkRow = rowLen, blkCol = colLen, blkBCol=colLen;
		int avaMemMap = (int) (mem*0.15) << 20; //Available memory space (bytes) in a single mapper, about 10~20% of total memory space, got from observation
		int avaMemReduce = (int) (mem*0.9) << 20; //Available memory space (bytes) in a single reducer
		if (method.compareTo("OPB") == 0)
		{
			//Outer-Product-based block approach, multiple column by multiple column each time
			//int blkRow = (rowLen>=1000)?1000:rowLen, blkCol = colLen/slots;
			blkRow = rowLen;
			blkCol = colLen/slots;
			//int blkRow = 1, blkCol = colLen; //Naive
			if (RAE)
			{
				if (colLen%slots != 0)
					blkCol++;
				if (colLen%blkCol == 0)
					slots = colLen/blkCol;
				//long estMemUsedinMap = blkRow*blkCol+blkCol*blkBCol+blkRow*blkBCol;
				long estMemUsedinMap = blkRow*blkCol+blkCol*blkRow+blkRow*blkRow;
				//long estMemUsedinReduce = blkRow*blkBCol*slots;
				long estMemUsedinReduce = blkRow*blkRow*slots;
				// Control the block size to maximize mappers performance
				while (estMemUsedinMap > avaMemMap/10 || estMemUsedinReduce > avaMemReduce/10 || rowLen%blkRow != 0)
				{
					blkRow -= 10;
					estMemUsedinMap = blkRow*blkCol+blkCol*blkRow+blkRow*blkRow;
					estMemUsedinReduce = blkRow*blkRow*slots;
				}
				estMemUsedinMap = blkRow*blkCol+blkCol*blkBCol+blkRow*blkBCol;
				estMemUsedinReduce = blkRow*blkBCol*slots;
				while (estMemUsedinMap > avaMemMap/10 || estMemUsedinReduce > avaMemReduce/10 || rowLen%blkBCol != 0)
				{
					blkBCol -= 10;
					estMemUsedinMap = blkRow*blkCol+blkCol*blkBCol+blkRow*blkBCol;
					estMemUsedinReduce = blkRow*blkBCol*slots;
				}
				blkRow=blkBCol=50;
				if (blkCol*(slots-1) > colLen)
					slots--;
				System.out.println("Master: esitmated block size is "+blkRow+" * "+blkCol+" * "+blkBCol+"for possible memory-constrained "+avaMemMap);
			}
		}
		else if (method.compareTo("IPB") == 0)
		{
			//Inner-Product-based block approach, multiple row by multiple column each time
			blkRow = rowLen/slots;
			blkCol = colLen;
			//conf.set("mapred.reduce.slowstart.completed.maps", "0.8");
			//slots = rowLen;
			if (RAE)
			{
				if (rowLen%slots != 0)
					blkRow++;
				if (rowLen%blkRow == 0)
					slots = rowLen/blkRow;
				long estMemUsedinMap = blkRow*blkRow+2*blkRow*blkCol;
				long estMemUsedinReduce = blkRow*blkRow*slots;
				// Control the block size to maximize mappers performance
				while (estMemUsedinMap > avaMemMap/10 || estMemUsedinReduce > avaMemReduce/10 || colLen%blkCol != 0)
				{
					blkCol -= 10;
					estMemUsedinMap = blkRow*blkRow+2*blkRow*blkCol;
					estMemUsedinReduce = blkRow*blkRow*slots;
				}
				estMemUsedinMap = blkRow*blkCol+blkCol*blkBCol+blkRow*blkBCol;
				estMemUsedinReduce = blkRow*blkBCol*slots;
				while (estMemUsedinMap > avaMemMap/10 || estMemUsedinReduce > avaMemReduce/10 || rowLen%blkBCol != 0)
				{
					blkBCol -= 10;
					estMemUsedinMap = blkRow*blkCol+blkCol*blkBCol+blkRow*blkBCol;
					estMemUsedinReduce = blkRow*blkBCol*slots;
				}
				System.out.println("Master: esitmated block size is "+blkRow+" * "+blkCol);
			}
		}
		else if (method.compareTo("naive") == 0)
		{
			//Naive method, one row by one column each time, just like we always did
			blkRow = 1;
			blkCol = colLen;
			//conf.set("mapred.reduce.slowstart.completed.maps", "0.8");
			if (rowLen < slots)
			{
				slots = rowLen;
				nNode = 1;
			}
			if (RAE)
			{
				int naiveRA = rowLen/slots;
				//System.out.println("Naive RA: "+naiveRA+" rows");
				conf.setInt("naiveRA", naiveRA); // how many rows will be processed in a Mapper if we applying RA enhancement
			}
			else
			{
				conf.setInt("naiveRA", 1); 
				slots = rowLen;
			}
		}
		else
		{
			System.out.println("Invalide method");
			return 0;
		}
		
		conf.set("mapred.create.symlink", "yes");
		conf.set("mapred.cache.files", "hdfs:///user/king/dump.sh#dump.sh");
		/*dump heap info. when OOM occur in order to have better analysis*/
		//conf.set("mapred.child.java.opts",  "-Xmx"+(mem*2)+"m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./myheapdump.hprof -XX:OnOutOfMemoryError=./dump.sh");
		conf.set("mapred.child.java.opts",  "-Xmx"+(mem*2)+"m ");
		conf.set("mapred.reduce.child.java.opts",  "-Xmx"+(mem*3)+"m ");
		conf.set("method", method);
		conf.setInt("blkRow",blkRow);
		conf.setInt("blkCol",blkCol);
		conf.setInt("blkBCol",blkBCol);
		conf.setInt("nSlot",slots);
		conf.setInt("rowLen",rowLen);
		conf.setInt("colLen",colLen);
		
		conf.set("io.sort.mb", mem+"");
		/*Increasing "io.sort.factor" could significantly reduce the # of 'spilled records', but not significant on overall performance*/
		conf.set("io.sort.factor", (mem/10)+"");
		conf.setInt("io.file.buffer.size", 65536);
		//conf.set("io.sort.spill.percent", "0.5");
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("fs.hdfs.impl.disable.cache","true");
		conf.set("mapred.reduce.slowstart.completed.maps", "0.8");
		//conf.set("mapred.job.reduce.input.buffer.percent", "0.5");
		//conf.setBoolean("mapred.map.tasks.speculative.execution", false);

		/*
		 * According to the official documents, "The right number of reduces seems to be 0.95 or 1.75 * (nodes * mapred.tasktracker.tasks.maximum). 
		 * At 0.95 all of the reduces can launch immediately and start transferring map outputs as the maps finish. 
		 * At 1.75 the faster nodes will finish their first round of reduces and launch a second round of reduces doing a much better job of load balancing."
		 * 
		 * In my case, it will be 0.95*18=17.1 or 1.75*18=31.5
		 * But I cannot feel the improvement
		 * 
		 * */
		
		int nBlk = (int) Math.ceil((double)(rowLen/blkRow));
		nBlk *= nBlk;
		//int numRe = (nBlk > nNode) ? nNode : nBlk;
		int numRe = nNode;
		conf.setInt("mapred.reduce.parallel.copies",numRe);
		//conf.set("io.sort.mb", "128");
		int good = 0, minTime;
		//long nMap = 0;
		fs.delete(new Path(args[2]),true);
		Job job = new Job(conf, method+"_DenseMatrixMultiplication_"+rowLen+"_"+colLen);
		job.setJarByClass(nativeMatMult.class);
		
		job.setMapOutputKeyClass(IntArrayWritable.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleArrayWritable.class);

		job.setMapperClass(Map.class);
		//job.setPartitionerClass(BlockPartitioner.class);
		//job.setCombinerClass(BlockCombiner.class);
		job.setReducerClass(Reduce.class);
				
		job.setNumReduceTasks(numRe);
		job.setInputFormatClass(MatrixInputFormat.class);
		job.setOutputFormatClass(MatrixOutputFormat.class);
		MatrixInputFormat.addInputPath(job, new Path(args[0]));
		MatrixInputFormat.addInputPath(job, new Path(args[1]));
		MatrixOutputFormat.setOutputPath(job, new Path(args[2]));
	
		good = (job.waitForCompletion(true))?1:0;
		//nMap = job.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "TOTAL_LAUNCHED_MAPS").getValue();
		//System.out.println("Launched mapper "+nMap);

		return good;
	}
}
