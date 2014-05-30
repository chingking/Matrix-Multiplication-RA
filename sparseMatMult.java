import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.lang.instrument.Instrumentation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import matrixFormat.*;

/*
 * Native matrix multiplication,
 *
 * King, Ching-Hsiang Chu
 * Started from 2014-05-08
 *
 */

public class sparseMatMult extends Configured implements Tool 
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

	public static class Map extends Mapper<IntArrayWritable, DoubleArrayWritable, IntWritable, IntDoubleMapWritable>
	{
		private static final Log LOG = LogFactory.getLog(Map.class);
		private int blkRow, blkCol, blkBCol;
		private float sparseA, sparseB;
		private boolean isOPB;	// false: IPB or naive, true: OPB
		private IntWritable outKey = new IntWritable();
		private IntDoubleMapWritable outVal;
		private HashMap<Integer, IntDoubleMapWritable> combinedOut;
		private long start=0, totalComp=0, totalIO=0;
		public void run(Context context) throws IOException, InterruptedException 
		{			
			LOG.info("Mapper.run(): Starting Mapper with "+Runtime.getRuntime().freeMemory()+" in "+Runtime.getRuntime().totalMemory()+" and "+Runtime.getRuntime().maxMemory());
		    setup(context);
		    try {
		    		//int cnt=0; 
			      while (context.nextKeyValue()) {
			        map(context.getCurrentKey(), context.getCurrentValue(), context);
			        //cnt++;
			        //System.out.println("SparseMapper.run(): Ending "+cnt+"th iteration with "+combinedOut.size()+" outputs and memory "+Runtime.getRuntime().freeMemory()+" in "+Runtime.getRuntime().totalMemory()+" and "+Runtime.getRuntime().maxMemory());
			        //if (combinedOut.size() > 2000)
			        	//flush(context);
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
			isOPB = (context.getConfiguration().get("method").compareTo("OPB")==0)?true:false; // false: IPB or naive, true: OPB
			blkRow = context.getConfiguration().getInt("blkRow",0);
			blkCol = context.getConfiguration().getInt("blkCol",0);
			blkBCol = context.getConfiguration().getInt("blkBCol",0);
			sparseA = 1.0f - (float)(context.getConfiguration().getInt("sparseA",0))/100;
			sparseB = 1.0f - (float)(context.getConfiguration().getInt("sparseB",0))/100;
			System.out.println("map(): R*C*C = "+blkRow+" * "+blkCol+" * "+blkBCol);
			//System.out.println("SparseMap(): Reserve "+(int)((blkBCol*sparseB)*1.2)+" for per iteration, "+(int)((blkRow*sparseA*blkBCol*sparseB)*1.2)+" for final");
			outVal = new IntDoubleMapWritable((int)((blkBCol*sparseB)*1.2), 1.0f);
			//combinedOut = new HashMap<Integer, IntDoubleMapWritable>((int)((blkRow*sparseA*blkBCol*sparseB)*1.2), 1.0f);
			//combinedOut = new HashMap<Integer, IntDoubleMapWritable>((int)((blkRow*sparseA)*1.2), 1.0f);
		}
		public void flush(Context context) throws IOException, InterruptedException
		{
			//System.out.println("SparseMap: flush");
			//flush output
			start = System.currentTimeMillis();
			  //System.out.println("SparseMap: Output "+finalOut.size());
			  //Iterator<Entry<IntWritable, IntDoubleMapWritable>> itfo = finalOut.entrySet().iterator();
			for (Entry<Integer, IntDoubleMapWritable> fo : combinedOut.entrySet())
			{
				//System.out.print(fo.getKey()+",");
				outKey.set(fo.getKey());
				context.write(outKey, fo.getValue());
			}
			System.out.println("SparseMap: elasped "+(System.currentTimeMillis()-start)+" ms to write out "+combinedOut.size());
			combinedOut.clear();
		}
		int combinCnt=0;
		public void combine(Context context) throws IOException, InterruptedException
		{
			if (combinedOut.containsKey(outKey.get()))
			{
				combinCnt++;
				//System.out.println("Combining row "+outKey.get()+", now we have "+combinedOut.size()+", "+(combinCnt++)+" rows are combined");
				//System.out.println("Combine row "+outKey.get());
				//System.out.println("\r\nSparseMap: putRecord key: "+outKey.get()+" from "+outVal.size()+" to "+finalOut.size()+": ");
				combinedOut.get(outKey.get()).addition(outVal);
				//context.write(outKey, combinedOut.get(outKey.get()));
				//combinedOut.remove(outKey.get());
			}
			else
			{				
				//System.out.println("\r\nSparseMap: new record "+outKey.get());
				combinedOut.put(outKey.get(), (IntDoubleMapWritable) outVal.clone());
			}
			outVal.clear();
		}
		long avg=0, max=0;	
		// Map is responsible for multiplying two small matrix
		public void map(IntArrayWritable key, DoubleArrayWritable value, Context context) throws IOException, InterruptedException
		{
			//System.out.println("map(): R*C = "+blkRow+" * "+blkCol+" with key "+key.toString()+", value length "+value.length());
			//System.out.println("map(): value "+value.toString());
			//LOG.info("Mapper(): Starting processing "+(cnt++)+" th record with key "+key.toString()+", value length "+value.length());
			//LOG.info("Mapper.map(): with available memory "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory()+". Block size: "+blkRow+"*"+blkCol);
			if (start > 0)
				totalIO += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();
			int localBoundary = key.get(0);
			value.setBaseVal(localBoundary);
			if (isOPB)
			{
				// For OPB, A is CSC format, B is CSR format
				outVal.time=0;
				//System.out.println("\r\nSparseMap: new round, "+finalOut.size());
				for (int i=2; i<localBoundary ; i+=2)
				{
					//outKey.set( (int) value.get(i));
					//outVal = new IntDoubleMapWritable(valLen);
					value.multiplyOPBSparseVector(i, localBoundary, outKey, outVal);
					//combine(context);
					context.write(outKey, outVal);
					//System.out.println("SparseMap: output, "+(outKey.get())+", "+outVal.toString());
				}
				//System.out.println("SparseMap: "+combinCnt+" duplicate rows are combined in this iteration");
				//combinCnt=0;
				long pt = (System.currentTimeMillis() - start);
				if (pt > max)
					max = pt;
				if (pt > 2*avg)
				{
					//System.out.println("SparseMap: elasped "+outVal.ftime+" ms on fetch, "+(outVal.time-outVal.ftime)+" ms on writing out");
					LOG.info("SparseMap,OBP: elasped "+(pt)+" ms (avg: "+avg+") on computation of "+value.length()+" and "+outVal.time+" ms to output "+outVal.size());
				}
				avg = (avg+pt)/2;
			}
			else
			{	
				// For IPB, A is CSR format, B is CSC format
				totalComp += System.currentTimeMillis() - start;
				outKey.set((int)value.get(0)); 
				outVal.clear();
				for (int i=1; i<localBoundary ; i+=2)
				{
					value.multiplyIPBSparseVector(i, outVal);
				}
				context.write(outKey, outVal);
				System.out.println("Mapper.run.IPB: "+(System.currentTimeMillis() - start)+" ms on computation\n");
				start = System.currentTimeMillis();
			}
			//LOG.info("Mapper(): Finished");
		}
	}
	
	public static class BlockCombiner extends Reducer<IntWritable, IntDoubleMapWritable, IntWritable, IntDoubleMapWritable>
	{
		//private static final Log LOG = LogFactory.getLog(BlockCombiner.class);
		private IntDoubleMapWritable output = new IntDoubleMapWritable(); 
		protected void reduce(IntWritable key, Iterable<IntDoubleMapWritable> values, Context context) throws IOException, InterruptedException
		{
			//System.out.println("Combiner.run(): Starting Combiner with "+Runtime.getRuntime().freeMemory()+" in "+Runtime.getRuntime().totalMemory()+" and "+Runtime.getRuntime().maxMemory());
			//long start = System.currentTimeMillis();
			output.clear();
			//int cnt=0;
			for (IntDoubleMapWritable val : values)
			{
				if (output.isEmpty())
					output.putAll(val);
				else
					output.addition(val);		
				//cnt++;
			}
			//if (cnt>1)
				//System.out.println("Combiner: combine row "+key.get()+" "+cnt+" times");
			context.write(key, output);
			//System.out.println("Combiner: "+(System.currentTimeMillis() - start)+" ms");
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, IntDoubleMapWritable, NullWritable, Text> 
	{
		private static final Log LOG = LogFactory.getLog(Reduce.class);
		private int blkBCol;
		private IntDoubleMapWritable output = new IntDoubleMapWritable(); 
		private Text finalOutput = new Text();
		//private Set<Long> indices;
		protected void setup(Context context)
		{
			blkBCol = context.getConfiguration().getInt("blkBCol",0);
			LOG.info("Reduce.run(): Starting Reduce with "+Runtime.getRuntime().freeMemory()+" in "+Runtime.getRuntime().totalMemory()+" and "+Runtime.getRuntime().maxMemory());
		}
		private String recoverRow(IntDoubleMapWritable row)
		{
			StringBuilder strb = new StringBuilder();
			for (int i=0 ; i<blkBCol ; i++)
			{
				if (row.containsKey(i))
					strb.append(row.get(i)+" ");
				else
					strb.append(0+" ");
			}
			return strb.toString();
		}
		protected void reduce(IntWritable key, Iterable<IntDoubleMapWritable> values, Context context) throws IOException, InterruptedException
		{
			//System.out.println("reduce(): R*C = "+blkRow+" * "+blkRow+", key "+key.toString());
			output.clear();
			finalOutput.clear();		
			long ptime, start = System.currentTimeMillis();
			int cnt=0;
			for (IntDoubleMapWritable val : values)
			{
				cnt++;
				if (output.isEmpty())
				{
					output.putAll(val);	
				}
				else
				{
					output.addition(val);
				}
			}
			ptime = System.currentTimeMillis() - start;
			System.out.println("Reducer: "+(ptime)+" ms for combining "+cnt+" sets of row "+key.get());
			//start = System.currentTimeMillis() ;
			finalOutput.set(key.get()+" "+recoverRow(output));
			context.write(NullWritable.get(), finalOutput);
			//System.out.println("SparseReduce: for row "+finalOutput.toString());
			
			//ptime = System.currentTimeMillis() - start;
			//System.out.println("Reducer: "+(ptime)+" ms for recovery and output");
		}
	}

	/*args: 0<Matrix A> 1<Matrix B> 2<Output directory> 3<Row length of A> 4<Column/Row length of A/B> 5<Column length of B> 
	 *      6<Sparsity of A> 7<Sparsity of B> 8<Method: naive, IPB, OPB> 9<Memory> 10<# of nodes>");
	*/
	public int run(String[] args) throws Exception 
	{
		int rowLen = Integer.parseInt(args[3]), colLen=Integer.parseInt(args[4]), colBLen=Integer.parseInt(args[5]);
		int sparseA = Integer.parseInt(args[6]), sparseB = Integer.parseInt(args[7]); // In fact, no need to know
		String method = args[8];
		int mem = Integer.parseInt(args[9]);
		int nNode = Integer.parseInt(args[10]);
				
		Configuration conf = super.getConf();//new Configuration();
		int slots = conf.getInt("mapred.tasktracker.map.tasks.maximum",18)*nNode;
		FileSystem fs = FileSystem.get(conf);
		//boolean RAE = true; //Enable Resource-aware Enhancement strategy or not
		//int avaMemMap = (int) (mem*0.15) << 20; //Available memory space (bytes) in a single mapper, about 10~20% of total memory space, got from observation
		//int avaMemReduce = (int) (mem*0.9) << 20; //Available memory space (bytes) in a single reducer
		int blkRow = rowLen, blkCol = colLen, blkBCol=colBLen;
		System.out.println("Input matrix size "+rowLen+" * "+colLen+" * "+colBLen);
		if (method.compareTo("OPB") == 0)
		{
			blkCol /= slots;
		}
		else if (method.compareTo("IPB") == 0)
		{
			blkRow /= slots;
		}
		else if (method.compareTo("naive") == 0)
		{
			blkRow /= slots;
			blkBCol /= slots;
		}
		else
		{
			System.out.println("Invalide method");
			return 0;
		}
		
		conf.set("mapred.create.symlink", "yes");
		conf.set("mapred.cache.files", "hdfs:///user/king/dump.sh#dump.sh");
		conf.set("mapred.child.java.opts",  "-Xmx"+(mem*2)+"m");
		conf.set("mapred.reduce.child.java.opts",  "-Xmx"+(mem*3)+"m");
		conf.set("method", method);
		conf.setBoolean("Sparse", true);
		conf.setInt("blkRow",blkRow);
		conf.setInt("blkCol",blkCol);
		conf.setInt("blkBCol",blkBCol);
		conf.setInt("sparseA",sparseA);
		conf.setInt("sparseB",sparseB);
		conf.setInt("nSlot",slots);
		conf.setInt("rowLen",rowLen);
		conf.setInt("colLen",colLen);
		conf.setInt("colBLen",colBLen);

		conf.set("io.sort.mb", (int)(mem)+"");
		/*Increasing "io.sort.factor" could significantly reduce the # of 'spilled records', but not significant on overall performance*/
		conf.set("io.sort.factor", (int)(mem*0.1)+"");
		conf.setFloat("io.sort.record.percent", (float) 0.01);
		conf.setInt("io.file.buffer.size", 65536);
		//conf.set("io.sort.spill.percent", "0.9");
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("fs.hdfs.impl.disable.cache","true");
		conf.set("mapred.reduce.slowstart.completed.maps", "0.8");
		//conf.set("min.num.spills.for.combine", "1000");
		//conf.set("mapred.job.reduce.input.buffer.percent", "0.5");
		//conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		
		//int nBlk = (int) Math.ceil((double)(rowLen/blkRow));
		//nBlk *= nBlk;
		//int numRe = (nBlk > nNode) ? nNode : nBlk;
		int numRe = nNode;
		conf.setInt("mapred.reduce.parallel.copies",slots);

		fs.delete(new Path(args[2]),true);
		Job job = new Job(conf, method+"_SparseMatrixMultiplication_"+rowLen+"_"+colLen+"_"+colBLen+"_"+sparseA+"_"+sparseB);
		job.setJarByClass(nativeMatMult.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntDoubleMapWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(BlockCombiner.class);
		job.setReducerClass(Reduce.class);
				
		job.setNumReduceTasks(numRe);
		job.setInputFormatClass(SparseMatrixInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		SparseMatrixInputFormat.addInputPath(job, new Path(args[0]));
		SparseMatrixInputFormat.addInputPath(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
	
		//nMap = job.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "TOTAL_LAUNCHED_MAPS").getValue();

		return (job.waitForCompletion(true))?1:0;
	}
}
