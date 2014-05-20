import java.io.*;
import java.net.URI;
import java.util.*;
import java.lang.instrument.Instrumentation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.partition.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapred.Task;

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
	public static class Map extends Mapper<Text, DoubleArrayWritable, LongWritable, MapWritable>
	{
		private static final Log LOG = LogFactory.getLog(Map.class);
		private int blkRow, blkCol, blkBCol;
		private String method;
		private String boundary[];
		private LongWritable outKey = new LongWritable();
		private MapWritable outVal = new MapWritable();
		private double val, valB;
		private IntWritable outVal_key = new IntWritable();
		private DoubleWritable outVal_val = new DoubleWritable();
		private long start=0, totalComp=0, totalIO=0;
		public void run(Context context) throws IOException, InterruptedException 
		{			
			Runtime rt = Runtime.getRuntime();
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
			method = context.getConfiguration().get("method");
			System.out.println("map(): R*C*C = "+blkRow+" * "+blkCol+" * "+blkBCol);
		}
		int cnt;
		// Map is responsible for multiplying two small matrix
		public void map(Text key, DoubleArrayWritable value, Context context) throws IOException, InterruptedException
		{
			//System.out.println("map(): R*C = "+blkRow+" * "+blkCol+" with key "+key.toString()+", value length "+value.length());
			//blkRow = (value.length()/2)/blkCol;
			//System.out.println("map(): value "+value.toString());
			//LOG.info("Mapper(): Starting processing "+(cnt++)+" th record with key "+key.toString()+", value length "+value.length());
			//LOG.info("Mapper.map(): with available memory "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory()+". Block size: "+blkRow+"*"+blkCol);
			if (start > 0)
				totalIO += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();
			boundary = key.toString().split(" ");
			int localBoundary=0, iterBoudary=0, index, indexB;
			for (int b=0 ; b<boundary.length ; b+=2)
			{
				localBoundary = Integer.parseInt(boundary[b]);
				if (method.compareTo("OPB") == 0)
				{
					// For OPB, A is CSC format, B is CSR format
					totalComp += System.currentTimeMillis() - start;
					start = System.currentTimeMillis();
					for (int i=iterBoudary+1; i<localBoundary ; i+=2)
					{
						outVal.clear();
						//indexB=value.find(boundary, value.get(i));
						index= (int) value.get(i);
						val = value.get(i+1);
						for (int j=localBoundary+1 ; j<iterBoudary; j+=2)
						{
							indexB= (int) value.get(j);
							valB = value.get(j+1);
							//System.out.println("SpareMap: "+index+", "+indexB+" "+(val*valB)+"("+val+"*"+valB+")");
							outVal_key.set(indexB);
							outVal_val.set(val*valB);
							outVal.put(outVal_key, outVal_val);
						}
						//strb.append("\r\n");
						outKey.set((long)index);
						context.write(outKey, outVal);
					}
				}
				else
				{	
					// For IPB, A is CSR format, B is CSC format
					totalComp += System.currentTimeMillis() - start;
					outKey.set((long)value.get(0)); 
					for (int i=1; i<localBoundary ; i+=2)
					{
						outVal.clear();
						//indexB=value.find(boundary, value.get(i));
						index=(int) value.get(i);
						val = value.get(i+1);
						valB = value.get(value.find(localBoundary+1, index));
						outVal.put(new IntWritable(index), new DoubleWritable(val*valB));
						//strb.append("\r\n");
					}
					context.write(outKey, outVal);
					//System.out.println("Mapper.run(): "+(System.currentTimeMillis() - start)+" ms on computation\n");
					start = System.currentTimeMillis();
				}
				iterBoudary = Integer.parseInt(boundary[b+1]);
			}
			//LOG.info("Mapper(): Finished");
		}
	}
	
	public static class Reduce extends Reducer<LongWritable, MapWritable, NullWritable, Text> 
	{
		private static final Log LOG = LogFactory.getLog(Reduce.class);
		//private DoubleArrayWritable out = new DoubleArrayWritable(), pout = new DoubleArrayWritable();
		private int blkBCol;
		private boolean doSum = false;
		private MapWritable output = new MapWritable(); 
		private Text finalOutput = new Text();
		private DoubleWritable tmp = new DoubleWritable();
		private IntWritable newIndex;
		private DoubleWritable entry;
		private Set<Writable> indices;
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
		private String recoverRow(MapWritable row)
		{
			StringBuilder strb = new StringBuilder();
			IntWritable cnt = new IntWritable();
			
			for (int i=0 ; i<blkBCol ; i++)
			{
				cnt.set(i);
				if (row.containsKey(cnt))
				{
					tmp = (DoubleWritable) row.get(cnt);
					strb.append(tmp.get()+" ");
				}
				else
				{
					strb.append(0+" ");
				}
			}
			return strb.toString();
		}
		protected void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
		{
			Runtime rt = Runtime.getRuntime();
			//LOG.info("Reduce.run(): Starting Reduce with "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory());
			//System.out.println("reduce(): R*C = "+blkRow+" * "+blkRow+", key "+key.toString());
			output.clear();
						
			for (MapWritable val : values)
			{
				finalOutput.clear();
				indices = val.keySet();
				for (Writable index : indices)
				{
					newIndex= (IntWritable)index;
					if (output.containsKey(newIndex))
					{						
						entry = (DoubleWritable) output.get(newIndex); //Original value
						tmp = (DoubleWritable) val.get(newIndex);		 //New entry to be combined with original entry in output
						entry.set(entry.get()+tmp.get());
						//System.out.println("SparseReduce (duplicate key): "+key.get()+", "+newIndex.get()+" "+entry.get());
						output.put(newIndex, entry);
					}
					else
					{
						tmp = (DoubleWritable) val.get(newIndex);
						//System.out.println("SparseReduce: "+key.get()+", "+newIndex.get()+", "+tmp.get());
						output.put(newIndex, tmp);
					}
				}		
			}
			finalOutput.set(recoverRow(output));
			//System.out.println("SparseReduce: for row "+key.get()+", "+finalOutput.toString());
			context.write(NullWritable.get(), finalOutput);
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
				
		Configuration conf = new Configuration();
		int slots = conf.getInt("mapred.tasktracker.map.tasks.maximum",18)*nNode;
		FileSystem fs = FileSystem.get(conf);
		//boolean RAE = true; //Enable Resource-aware Enhancement strategy or not
		//int avaMemMap = (int) (mem*0.15) << 20; //Available memory space (bytes) in a single mapper, about 10~20% of total memory space, got from observation
		//int avaMemReduce = (int) (mem*0.9) << 20; //Available memory space (bytes) in a single reducer
		int blkRow = rowLen, blkCol = colLen, blkBCol=colBLen;
		if (method.compareTo("OPB") == 0)
		{
			blkCol /= slots;
		}
		else if (method.compareTo("IPB") == 0)
		{
			blkRow /= slots;
			blkBCol /= slots;
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
		conf.set("mapred.child.java.opts",  "-Xmx"+(mem*2)+"m ");
		conf.set("mapred.reduce.child.java.opts",  "-Xmx"+(mem*3)+"m ");
		conf.set("method", method);
		conf.setBoolean("Sparse", true);
		conf.setInt("blkRow",blkRow);
		conf.setInt("blkCol",blkCol);
		conf.setInt("blkBCol",blkBCol);
		conf.setInt("nSlot",slots);
		conf.setInt("rowLen",rowLen);
		conf.setInt("colLen",colLen);
		conf.setInt("colBLen",colBLen);
		
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
		
		int nBlk = (int) Math.ceil((double)(rowLen/blkRow));
		nBlk *= nBlk;
		//int numRe = (nBlk > nNode) ? nNode : nBlk;
		int numRe = nNode;
		conf.setInt("mapred.reduce.parallel.copies",numRe);
		//conf.set("io.sort.mb", "128");
		int good = 0, minTime;
		//long nMap = 0;
		fs.delete(new Path(args[2]),true);
		Job job = new Job(conf, method+"_SparseMatrixMultiplication_"+rowLen+"_"+colLen+"_"+colBLen+"_"+sparseA+"_"+sparseB);
		job.setJarByClass(nativeMatMult.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
				
		job.setNumReduceTasks(numRe);
		job.setInputFormatClass(SparseMatrixInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		SparseMatrixInputFormat.addInputPath(job, new Path(args[0]));
		SparseMatrixInputFormat.addInputPath(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
	
		good = (job.waitForCompletion(true))?1:0;
		//nMap = job.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "TOTAL_LAUNCHED_MAPS").getValue();

		return good;
	}
}