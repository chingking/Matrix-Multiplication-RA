package matrixFormat;

import org.apache.hadoop.io.*;

import java.util.*;
import java.io.*;

/*Customized efficient (storage & computation) data type to store Sparse matrix*/
public class IntDoubleMapWritable extends HashMap<Integer, Double> implements WritableComparable<IntDoubleMapWritable> {
	public IntDoubleMapWritable() {}
    public IntDoubleMapWritable(int initCapacity)
    {
    	super(initCapacity);
    }
    public IntDoubleMapWritable(int initCapacity, float loadFactor)
    {
    	super(initCapacity,loadFactor);
    }
    public IntDoubleMapWritable(IntDoubleMapWritable input)
    {
    	super(input);
    }
	public void allMultiply(double val)
    {
    	//System.out.println("IntDoubleMapWritable: "+val+" * "+this.toString());
    	for (Map.Entry<Integer, Double> entry : entrySet())
    	{
    		entry.setValue(val*entry.getValue().doubleValue());
    		//put(entry.getKey(), val*entry.getValue().doubleValue());
    	}
    	//System.out.println("IntDoubleMapWritable: Become "+this.toString());
    }
	public void addition(IntDoubleMapWritable input)
	{
		//System.out.println("IntDoubleMapWritable: Add ("+input.size()+")"+input.toString());
		//System.out.println("IntDoubleMapWritable: To ("+size()+")"+this.toString());
		for (Map.Entry<Integer, Double> entry : input.entrySet())
		{
			Integer k = entry.getKey();
			if (this.containsKey(k))
			{
				//System.out.print("IntDoubleMapWritable: Found duplicate column "+k+" from "+get(k).doubleValue()+" to add "+entry.getValue().doubleValue()+" to be ");
				this.put(k, this.get(k).doubleValue()+entry.getValue().doubleValue());
				//System.out.println(get(k).doubleValue());
			}
			else
				this.put(k, entry.getValue());
		}
		//System.out.println("IntDoubleMapWritable: Become ("+size()+")"+this.toString());
	}

	public String toString()
	{
		String strb = new String(); 
		for (Map.Entry<Integer, Double> entry : entrySet()) {
			strb += ("<"+entry.getKey().intValue()+","+entry.getValue().doubleValue()+">, ");
        }
		return strb;
	}
    public long ftime=0, time=0;
	//Implementation of WritableComparable
    @Override
    public void write(DataOutput out) throws IOException {
    	long start = System.currentTimeMillis();
		WritableUtils.writeVInt(out, size());
		for (Map.Entry<Integer,Double> entry : entrySet())
		{
			WritableUtils.writeVInt(out, entry.getKey());
			out.writeDouble(entry.getValue());
		}
		//System.out.println("Took "+(System.currentTimeMillis()-start)+" ms to write");
		time+=System.currentTimeMillis()-start;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int size = WritableUtils.readVInt(in);
        for (int i=0;i<size;i++) {
            //put(WritableUtils.readVLong(in), in.readDouble());
        	put(WritableUtils.readVInt(in), in.readDouble());
        }
    }
    /*public static IntDoubleMapWritable read(DataInput in) throws IOException 
    {
    	IntDoubleMapWritable result = new IntDoubleMapWritable();
        result.readFields(in);
        return result;
    }*/
	@Override
	public int compareTo(IntDoubleMapWritable arg0) 
	{
		int comp=0;
		for (Integer index : keySet())
		{
			if (!arg0.containsKey(index))
			{
				comp++;
			}
			else
			{
				comp += (int)(this.get(index) - arg0.get(index));
			}
		}
		return comp;
		//return 0; //Sorting is not required
	}
}
