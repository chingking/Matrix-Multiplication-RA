
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class matrixTransform  extends Configured implements Tool 
{
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		boolean CSR;
		Text output = new Text();
		protected void setup(Context context)
		{
			CSR = context.getConfiguration().getBoolean("CSR", CSR);
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String [] row = value.toString().split(" ");
			long rowID = Long.parseLong(row[0]);
			if (CSR)
			{
				StringBuilder out = new StringBuilder();
				key.set(rowID);
				for (int i=1 ; i<row.length ; i++)
				{
					if (row[i].compareTo("0") != 0)
					{
						out.append((i-1)+" "+row[i]+" ");
					}
				}
				output.set(out.toString());
				context.write(key, output);
			}
			else
			{
				for (int i=1 ; i<row.length ; i++)
				{
					if (row[i].compareTo("0") != 0)
					{
						key.set(i-1);
						output.set(rowID+" "+row[i]+" ");
						context.write(key, output);
					}
				}
			}
			
		}
	}
	public static class Reduce extends Reducer<LongWritable, Text, NullWritable, Text> 
	{
		boolean CSR;
		private MultipleOutputs<NullWritable, Text> mos;
		 @Override
		 public void setup(final Context context)
		 {
			 mos = new MultipleOutputs<NullWritable, Text>(context);
			 CSR = context.getConfiguration().getBoolean("CSR", false);
		 }

		 @Override
		 public void cleanup(final Context context) throws IOException, InterruptedException
		 {
			 mos.close();
		 }
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (CSR)
			{
				for (Text val : values)
				{
					mos.write(NullWritable.get(), new Text(key.get()+" "+val.toString()), "CSR");
					context.progress();
					//context.write(NullWritable.get(), new Text(key.get()+" "+val.toString()));
				}
			}
			else
			{
				StringBuilder strb = new StringBuilder();
				//strb.append(key+" ");
				for (Text val : values)
				{
					strb.append(val.toString());
				}
				mos.write(NullWritable.get(), new Text(key.get()+" "+strb.toString()), "CSC");
				context.progress();
			}
		}
	}
	public int run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		
		conf.set("mapred.create.symlink", "yes");
		conf.set("mapred.child.java.opts",  "-Xmx768m ");
		conf.set("io.sort.mb", "384");
		conf.set("io.sort.spill.percent", "0.9");
		conf.setLong("io.file.buffer.size", 65536);
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("fs.hdfs.impl.disable.cache","true");
		//conf.set("mapred.reduce.slowstart.completed.maps", "0.8");
		conf.set("outputfilename", args[0]+".sparse");
		conf.set("method", args[0]);
		boolean CSR=(args[0].compareTo("OPB")==0)?true:false; //If second matrix to be CSR ? ; CSR: Compressed Sparse Row; otherwise, Compressed Sparse Column
		//Generally, CSR require less time to complete. Therefore, submit CSR job first, then immediately submit CSC job
		conf.setBoolean("CSR",CSR);
		if (!CSR)
		{
			String tmp = args[1];
			args[1] = args[2];
			args[2] = tmp;
			CSR = !CSR;
		}
		int good = 0;
		for (int i=args.length-1 ; i>0 ; i--)
		{
			conf.setBoolean("CSR",CSR);
			Job job = new Job(conf, "matrixTransform_"+((CSR)?"CSR_":"CSC_")+args[i]);
			job.setJarByClass(matrixTransform.class);
	
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
	
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
					
			job.setNumReduceTasks(1);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			TextInputFormat.addInputPath(job, new Path(args[i]));
			TextOutputFormat.setOutputPath(job, new Path(args[i]+"_sparse"));
			//Generally, CSR require less time to complete. Therefore, submit CSR job first, then immediately submit CSC job
			if (i==args.length-1)
				job.submit();
			else
			{
				good = (job.waitForCompletion(true))?1:0;
				if(good==0)
					break;
			}
			CSR = !CSR;
		}
		return good;
	}
}
