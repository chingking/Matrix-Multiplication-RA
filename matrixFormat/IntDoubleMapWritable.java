package matrixFormat;

import org.apache.hadoop.io.*;

import java.util.*;
import java.io.*;

/*Customized efficient (storage & computation) data type to store Sparse matrix*/
public class IntDoubleMapWritable implements WritableComparable<IntDoubleMapWritable> {
	public class Entry {
		private int key;
		private double value;
		public Entry(int k, double v)
		{
			key = k;
			value = v;
		}
		public Entry(Entry e)
		{
			key = e.getKey();
			value = e.getValue(); 
		}
		public void setKey(int k)
		{
			key = k;
		}
		public int getKey()
		{
			return key;
		}
		public void setValue(double v)
		{
			value = v;
		}
		public double getValue()
		{
			return value;
		}
		public void multiplyVal(double v)
		{
			value*=v;
		}
		public void addVal(double v)
		{
			value+=v;
		}
	}
	
	private Entry entry[];
	private int assignedPosn = 0;
	public IntDoubleMapWritable() {}
    public IntDoubleMapWritable(int initCapacity)
    {
    	setEntry(new Entry[initCapacity]);
    	assignedPosn = 0;
    }
    public IntDoubleMapWritable(IntDoubleMapWritable input)
    {
    	copy(input);
    }
    public IntDoubleMapWritable(String input)
    {
    	set(input);
    }
    public void copy(IntDoubleMapWritable input)
    {
    	setEntry(new Entry[input.length()]);
    	entry = Arrays.copyOf(input.getEntry(), input.length());
    	assignedPosn = input.length();
    }
    public void ensureCapacity(int length)
	{
		int oldCapacity = entry.length;
		if (oldCapacity < assignedPosn+length)
		{
			int newCapacity = (int) ((oldCapacity+length)*1.2); // Increase capacity slowly to prevent out of memory
			entry = Arrays.copyOf(entry, newCapacity);
		}
	}
    public Entry getEntry(int index)
    {
    	return entry[index];
    }
    public void put(int k, double v)
    {
    	ensureCapacity(1);
    	entry[assignedPosn] = new Entry(k, v);
    	assignedPosn++;
    }
    public int size()
    {
    	return entry.length;
    }
    public int length()
    {
    	return assignedPosn;
    }
    public int findKey(int k)
    {
    	int i = 0;
    	for (; i<length() && entry[i].getKey()==k ; i++){}
    	return (i==length())?-1:i;
    }
    public void set(String input)
    {
    	ensureCapacity(input.length());
    	String row[] = input.split(" ");
    	for (int i=1 ; i<row.length ; i+=2)
    		put(Integer.parseInt(row[i]), Double.parseDouble(row[i+1]));
    }
    public double get(int index)
    {
    	return entry[index].getValue();
    }
    public void clear()
    {
    	if (entry != null)
    		setEntry(new Entry[entry.length]);
    	else
    		setEntry(new Entry[1]);
    	assignedPosn = 0;
    }
    public boolean isEmpty()
    {
    	return (assignedPosn>0)?true:false;
    }
	public void allMultiply(double val)
    {
    	//System.out.println("IntDoubleMapWritable: "+val+" * "+this.toString());
    	for (Entry e : entry)
    	{
    		e.multiplyVal(val);
    	}
    	//System.out.println("IntDoubleMapWritable: Become "+this.toString());
    }
	public void singleAdd(int index, double v)
	{
		entry[index].addVal(v);
	}
	public int combine(IntDoubleMapWritable input)
	{
		//System.out.println("IntDoubleMapWritable: Add ("+input.size()+")"+input.toString());
		//System.out.println("IntDoubleMapWritable: To ("+size()+")"+this.toString());
		int originLength = this.length();
		int k, index;
		long start = System.currentTimeMillis();
		for (Entry e : input.getEntry())
		{
			k = e.getKey();
			if ((index = findKey(k)) >=0)
			{
				//System.out.print("IntDoubleMapWritable: Found duplicate column "+k+" from "+get(k).doubleValue()+" to add "+entry.getValue().doubleValue()+" to be ");
				entry[index].addVal(e.getValue());
				//System.out.println(get(k).doubleValue());
			}
			else
				this.put(k, e.getValue());
		}
		combineTime=System.currentTimeMillis()-start;
		//System.out.println("IntDoubleMapWritable: Become ("+size()+")"+this.toString());
		return this.length()-originLength;
	}

	public String toString()
	{
		String strb = new String(); 
		for (Entry e : entry){
			strb += ("<"+e.getKey()+","+e.getValue()+">, ");
        }
		return strb;
	}
    public long ftime=0, time=0, combineTime=0;
	//Implementation of WritableComparable
    @Override
    public void write(DataOutput out) throws IOException {
    	long start = System.currentTimeMillis();
		WritableUtils.writeVInt(out, length());
		for (int i=0 ; i<length() ; i++)
		{
			WritableUtils.writeVInt(out, entry[i].getKey());
			out.writeDouble(entry[i].getValue());
		}
		//System.out.println("Took "+(System.currentTimeMillis()-start)+" ms to write");
		time=System.currentTimeMillis()-start;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        //clear();
        int size = WritableUtils.readVInt(in);
        setEntry(new Entry[size]);
        assignedPosn = 0;
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
		int comp=0, index;
		for (Entry e : entry)
		{
			index = e.getKey();
			if ((index = arg0.findKey(index)) < 0)
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
	public Entry[] getEntry() {
		return Arrays.copyOf(entry, length());
	}
	public void setEntry(Entry entry[]) {
		this.entry = entry;
	}
}
