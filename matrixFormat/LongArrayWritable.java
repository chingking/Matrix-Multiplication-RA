package matrixFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class LongArrayWritable implements WritableComparable<LongArrayWritable>
{
	private long values[];
	private int assignPosn;
	public LongArrayWritable()
	{
		values=new long[1];
		assignPosn=0;
	}
	public LongArrayWritable(int length)
	{
		values=new long[length];
	}
	public LongArrayWritable(long input[]){
		values=input;
	}
	public LongArrayWritable(String input){
		set(input);
	}
	public void ensureCapacity(int length)
	{
		int oldCapacity = values.length;
		if (oldCapacity < assignPosn+length)
		{
			int newCapacity = (int) ((oldCapacity+length)*1.2);
			values = Arrays.copyOf(values, newCapacity);
		}
	}
	public void set(String input)
	{
		if (input.length() == 0)
			return;
		String row[] = input.split(" ");
		values = new long[row.length];
		assignPosn = row.length;
		for (int i=0 ; i<row.length ; i++)
		{
			values[i]=Long.parseLong(row[i]);
		}
	}
	public void set(long input[])
	{
		values=input;
		assignPosn=values.length;
	}
	public void add(long input)
	{
		ensureCapacity(1);
		values[assignPosn++]=input;
	}
	public void add(long input[])
	{
		ensureCapacity(input.length);
		for (long i : input)
		{
			values[assignPosn++]=i;
		}
	}
	public void add(String input)
	{
		if (input.length()==0)
			return;
		String entry[] = input.split(" ");
		ensureCapacity(entry.length);
		for (String i : entry)
		{
			values[assignPosn++]=Long.parseLong(i);
		}
	}
	/*Get the values from the sparese representation. e.g. "0 5.6 1 4.5 2 3.3", 0 1 2 are the keys we will fetch */
	public void addSpareKey(String input)
	{
		if (input.length()==0)
			return;
		String entry[] = input.split(" ");
		ensureCapacity(entry.length/2);
		for (int i=1 ; i<entry.length && entry[i]!=" " ; i+=2)
		{
			values[assignPosn++]=Long.parseLong(entry[i]);
		}
	}
	public int length()
	{
		return this.assignPosn;
	}
	public void clear()
	{
		values=new long[1];
		assignPosn=0;
	}
	public long get(int index)
	{
		return values[index];
	}
	  ////////////////////////////////////////////
	  // Writable methods
	  ////////////////////////////////////////////

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		values = new long[size];
		for (int i = 0; i <size ; i++)
		{
		    values[i] = in.readLong();                          // store it in values
		}
		this.assignPosn = size;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.length());                 // write values
	    for (int i = 0; i < this.length(); i++) {
	    	out.writeLong(values[i]);
	    }
	}
	@Override
	public int compareTo(LongArrayWritable arg0) {
		int i;
		for (i=0 ; i<length() && i<arg0.length() && this.values[i] == arg0.get(i); i++){}
		return (int) (this.values[i] - arg0.get(i));
	}

}
