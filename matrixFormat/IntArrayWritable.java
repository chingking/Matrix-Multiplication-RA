package matrixFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class IntArrayWritable implements WritableComparable<IntArrayWritable>
{
	private int values[];
	private int assignPosn;
	public IntArrayWritable()
	{
		values=new int[1];
		assignPosn=0;
	}
	public IntArrayWritable(int length)
	{
		values=new int[length];
	}
	public IntArrayWritable(int input[]){
		values=input;
	}
	public IntArrayWritable(String input){
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
		values = new int[row.length];
		assignPosn = row.length;
		for (int i=0 ; i<row.length ; i++)
		{
			values[i]=Integer.parseInt(row[i]);
		}
	}
	public void set(int input[])
	{
		values=input;
		assignPosn=values.length;
	}
	public void add(int input)
	{
		ensureCapacity(1);
		values[assignPosn++]=input;
	}
	public void add(int input[])
	{
		ensureCapacity(input.length);
		for (int i : input)
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
			values[assignPosn++]=Integer.parseInt(i);
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
			values[assignPosn++]=Integer.parseInt(entry[i]);
		}
	}
	public int length()
	{
		return this.assignPosn;
	}
	public void clear()
	{
		values=new int[1];
		assignPosn=0;
	}
	public int get(int index)
	{
		return values[index];
	}
	
	  ////////////////////////////////////////////
	  // Writable methods
	  ////////////////////////////////////////////

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		values = new int[size];
		for (int i = 0; i <size ; i++)
		{
		    values[i] = in.readInt();                          // store it in values
		}
		this.assignPosn = size;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.length());                 // write values
	    for (int i = 0; i < this.length(); i++) {
	    	out.writeInt(values[i]);
	    }
	}
	@Override
	public int compareTo(IntArrayWritable arg0) {
		int i;
		for (i=0 ; i<length() && i<arg0.length() && this.values[i] == arg0.get(i); i++){}
		if (i>0)
			i--;
		return (int) (this.values[i] - arg0.get(i));
	}

}
