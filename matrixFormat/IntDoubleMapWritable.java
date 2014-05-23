package matrixFormat;

import org.apache.hadoop.io.*;

import java.util.*;
import java.io.*;

/*Customized efficient (storage & computation) data type to store Sparse matrix*/
public class IntDoubleMapWritable extends HashMap<Integer, Double> implements WritableComparable<IntDoubleMapWritable> {
	
    public IntDoubleMapWritable() {}
    
	public void summation(IntDoubleMapWritable input)
	{
		Set<Integer> targetIndices = input.keySet();
		for (Integer index : targetIndices)
		{
			if (this.containsKey(index))
				this.put(index, this.get(index)+input.get(index));
			else
				this.put(index, input.get(index));
		}
	}
	public String toString()
	{
		String str = new String(); 
		for (Integer k : keySet()) {
			str += ("<"+k+","+get(k)+">, ");
        }
		return str;
	}
    
    //Implementation of WritableComparable
    @Override
    public void write(DataOutput out) throws IOException {
    	WritableUtils.writeVInt(out, size());
        for (Integer k : keySet()) {
        	WritableUtils.writeVInt(out,k);
        	out.writeDouble(get(k));
        }
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
		/*	int comp=0;
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
		return comp;*/
		return 0; //Sorting is not required
	}
}