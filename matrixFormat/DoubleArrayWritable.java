package matrixFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleArrayWritable implements WritableComparable<DoubleArrayWritable>
{ 
	private static final Log LOG = LogFactory.getLog(DoubleArrayWritable.class);
	//private ArrayList<DoubleWritable> values;
	//private ArrayList<Double> values;
	private double[] values = new double[0];
	private int assignPosn = 0;
	public DoubleArrayWritable() { } 
	public DoubleArrayWritable(double[] input) 
	{
		
		values = input;
	}
	public DoubleArrayWritable(ArrayList<Double> input) 
	{
		
		values = new double[input.size()*2];
		for (int i=0 ; i<input.size() ; i++)
			values[i] = input.get(i);
	}
	public DoubleArrayWritable(DoubleArrayWritable input) 
	{
		
		values = input.getAll();
	}
	public DoubleArrayWritable(String input) 
	{
		set(input);
	}
	public DoubleArrayWritable(int length) 
	{
		
		values = new double[length];
		
		//for (int i=0 ; i<length ; i++)
		//	values[i]=0;
		//values.trimToSize();
	}
	public void set(DoubleArrayWritable input)
	{
		values = Arrays.copyOf(input.getAll(),input.getAll().length);
		assignPosn = input.length();
		//if (values == null)
		//	values = new ArrayList<Double>(input.length());
		//add(input);
	}
	/*public void set(ArrayList<Double> input)
	{
		values = input;
	}*/
	public void set(String input)
	{
		if (input.length() == 0)
			return;
		String row[] = input.split(" ");
		values = new double[row.length];
		assignPosn = row.length;
		for (int i=0 ; i<row.length ; i++)
		{
			values[i]=Double.parseDouble(row[i]);
		}
	}
	public void set(double input[][])
	{
		if (input.length == 0)
			return;
		values = new double[input.length*input[0].length];
		assignPosn = input.length*input[0].length;
		for (int i=0 ; i<input.length ; i++)
		{
			for (int j=0 ; j<input[i].length ; j++)
				values[i*input.length+j] = input[i][j];
		}
	}
	public void set(int index, double val)
	{
		if (values == null)
			values = new double[1];
		values[index] = val;
	}
	public void set(double[] in)
	{
		values = Arrays.copyOf(in, in.length);
		assignPosn = in.length;
		//values = in;
	}
	/*public void set (Object []in)
	{
		values = new ArrayList<Double>(in.length);
		for (int i=0 ; i<in.length ; i++)
			values.add((Double) in[i]);
	}*/
	
	public void ensureCapacity(int length)
	{
		int oldCapacity = values.length;
		if (oldCapacity < assignPosn+length)
		{
			int newCapacity = (int) ((oldCapacity+length)*1.2);
			values = Arrays.copyOf(values, newCapacity);
		}
	}
	
	public void add(int length, double value)
	{
		if (values == null)
			values = new double[length];
		ensureCapacity(length);
		for (int i=0; i<length ; i++)
		{
				values[assignPosn++] = value;
		}
	}
	public void add(double in)
	{
		try
		{
			if (values == null)
				values = new double[1];
			ensureCapacity(1);
			values[assignPosn++] = in;
		} catch (OutOfMemoryError oom)
		{	
			Runtime rt = Runtime.getRuntime();
			System.out.println("DoubleArrayWritable(): OOM with "+rt.freeMemory()+" in "+rt.totalMemory()+" and "+rt.maxMemory());
		}
	}
	public void add(DoubleArrayWritable in)
	{
		if (values == null)
			values = new double[in.length()];
		ensureCapacity(in.length());
		for (double i : in.values)
			values[assignPosn++] = i;
	}
	public void add(byte buf[], int start, int length)
	{
		if (values == null)
			values = new double[length];
		ensureCapacity(length);
		byte num[] = new byte[length];
  	  	for (int i=0 ; i<length ; i++)
  	  		num[i]=buf[i+start];
  	  	String tmp=new String(num);
  	  	if (!tmp.isEmpty())
  	  		values[assignPosn++] = Double.parseDouble(tmp);
	}

	public void add(double input[])
	{
		if (input.length == 0)
			return;
		if (values == null)
			values = new double[input.length];
		ensureCapacity(input.length);
		for (double i : input)
			values[assignPosn++] = i;
	}
	public void add(String input)
	{
		if (input.length() == 0)
			return;
		String row[] = input.split(" ");
		ensureCapacity(row.length);
		//System.out.println("DoubleArrayWritable.add(): Got an input "+input.length()+", has "+row.length);
		for (String i : row)
		{
			//System.out.print(" "+row[i]);
			values[assignPosn++] = Double.parseDouble(i);
		}
	}
	/*Get the values from the sparese representation. e.g. "0 5.6 1 4.5 2 3.3", 5.6 4.5 3.5 are the values we will fetch */
	public void addSparseValue(String input)
	{
		if (input.length() == 0)
			return;
		String row[] = input.split(" ");
		ensureCapacity(row.length/2);
		//System.out.println("DoubleArrayWritable.addSparse(): Got an input "+input.length()+", has "+row.length);
		for (int i=1 ; i<row.length && row[i]!=" " ; i+=2)
		{
			//System.out.print(" "+row[i]);
			values[assignPosn++] = Double.parseDouble(row[i]);
		}
	}
	public void sum(DoubleArrayWritable input)
	{
		if (values.length==0)
			set(input);
		else
		{
			for (int i=0 ; i < values.length ; i++)
				values[i] = values[i] + input.get(i);
		}
	}
	
	public String toString()
	{
		//LOG.info("DoubleArrayWritable(): start to converting DArray to string ");
		//String str = new String();
		StringBuilder str = new StringBuilder();
		BigDecimal bd;
		for (int i=0 ; i<this.assignPosn ; i++)
		{
			bd = new BigDecimal(values[i]);
			str.append(bd.setScale(3, BigDecimal.ROUND_CEILING).toString()+" ");
		}
		
		return str.toString();
	}
	 
	public void clear()
	{
		//values =  new ArrayList<Double>();
		values = new double[0];
		assignPosn = 0;
		baseVal = null;
	}
	public void clear(int length)
	{
		values =  new double[length];
		assignPosn = 0;
		//for (int i=0 ; i<length ; i++)
		//	set(i, 0.0);
		//values.trimToSize();
	}
	public void setPos(int newPos)
	{
		assignPosn=newPos;
	}
	
	public double[] getAll()
	{
		return values;
	}
	
	public void get(DoubleArrayWritable val, int start, int len)
	{
		val.set(this.get(start,len));
	}
	public DoubleArrayWritable getDArray(int start, int len)
	{
		DoubleArrayWritable out = new DoubleArrayWritable(this.get(start, len));
		//System.out.println("DoubleArrayWritable.getDArray(): Get "+out.length()+" elements");
		return out;
	}
	public double[] get(int start, int len)
	{
		if (len == -1)
			len = length()-start;
		double [] out = new double[len];
		for (int i = 0 ; i<len ; i++)
			out[i] = values[i+start];
		return out;
	}
	public double get(int index)
	{
		return values[index];
	}
	public int find(int fromIndex, double val)
	{
		int i=fromIndex;
		for (; i< this.length() && this.get(i) != val ; i++)
		;
		return i;
	}
	public void addition(int index, double value)
	{
		values[index] += value;
	}
	public double multiplyEntry(int index1, int index2)
	{
		return values[index1]*values[index2];
	}
	private IntDoubleMapWritable baseVal;
	public void setBaseVal(int boundary)
	{
		if (baseVal == null)
			baseVal = new IntDoubleMapWritable((length()-boundary-1)/2, 1.0f);
		baseVal.clear();
		for (int i=boundary+1 ; i<length() ; i+=2)
			baseVal.put((int)values[i], values[i+1]);
	}
	public void multiplyOPBSparseVector(int valIndex, int boundary, IntWritable key, IntDoubleMapWritable val)
	{
		val.clear();
		val.putAll(baseVal);
		key.set((int)values[valIndex-1]);
		double target = values[valIndex];
		val.allMultiply(target);
		//for (int i=boundary+1 ; i<length() ; i+=2)
			//val.put((int)values[i], target*values[i+1]);
	}
	public void multiplyIPBSparseVector(int valIndex, IntDoubleMapWritable val)
	{
		if (baseVal.containsKey(valIndex))
		{
			val.put(valIndex, baseVal.get(valIndex)*values[valIndex+1]);
		}
	}
	/*Multiply two matrices store in values*/
	public DoubleArrayWritable multiply(int blkRow, int blkCol, int blkBCol)
	{
		DoubleArrayWritable out = new DoubleArrayWritable(blkRow*blkBCol);
		int dim = blkRow*blkCol;
		//double tmpSum=0;
		for (int i=0 ; i<blkRow ; i++ )	
			for (int k=0 ; k<blkCol ; k++)
			{
				for (int j=0 ; j<blkBCol ; j++)
					out.addition( i*blkBCol+j, values[i*blkCol+k]*values[dim+k*blkBCol+j] );
			}
		out.assignPosn = blkRow*blkBCol;
		//System.out.println("DoubleArray.multiply(): output "+out.length()+", real length "+out.values.length);
		//out.printResult(blkRow, blkBCol);
		return out;
	}
	public double getMult(int index1, int index2)
	{
		return values[index1]*values[index2];
	}
	public int length()
	{
		//return values.length;
		return this.assignPosn;
	}
	/*For debug usage*/
	public void printResult(int blkRow, int blkBCol)
	{
		System.out.println("\r\nResult C");
		for (int i=0 ; i<blkRow ; i++)
		{
			for (int j=0 ; j<blkBCol ; j++)
				System.out.print(values[i*blkBCol+j]+" ");
			System.out.println("");
		}
	}
	public void printMatrix(int blkRow, int blkCol, int blkBCol)
	{
		System.out.println("Block A");
		for (int i=0 ; i<blkRow ; i++)
		{
			for (int j=0 ; j<blkCol ; j++)
				System.out.print(values[i*blkCol+j]+" ");
			System.out.println("");
		}
		System.out.println("\r\nBlock B");
		for (int i=0 ; i<blkCol ; i++)
		{
			for (int j=0 ; j<blkBCol ; j++)
				System.out.print(values[blkRow*blkCol+i*blkBCol+j]+" ");
			System.out.println("");
		}
	}
	  ////////////////////////////////////////////
	  // Writable methods
	  ////////////////////////////////////////////

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		values = new double[size];
		for (int i = 0; i <size ; i++)
		{
			//Double value = new Double();
			//value.readFields(in);                       // read a value
		    values[i] = in.readDouble();                          // store it in values
		}
		this.assignPosn = size;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.length());                 // write values
	    for (int i = 0; i < this.length(); i++) {
	    	out.writeDouble(values[i]);
	    }
	}
	@Override
	public int compareTo(DoubleArrayWritable arg0) {
		int i;
		for (i=0 ; i<length() && i<arg0.length() && this.values[i] == arg0.get(i); i++){}
		return (int) (this.values[i] - arg0.get(i));
	}
}
