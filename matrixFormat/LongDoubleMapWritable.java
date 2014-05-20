package matrixFormat;

import org.apache.hadoop.io.*;

import java.util.*;
import java.io.*;

public class LongDoubleMapWritable extends HashMap<Long, Double> implements Writable {
    public LongDoubleMapWritable() { }
    //Implementation of WritableComparable
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size());
        for (Long k : keySet()) {
            out.writeLong(k);
            out.writeDouble(get(k));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int size = in.readInt();
        for (int i=0;i<size;i++) {
        	Long k = in.readLong();
            double v = in.readDouble();
            put(k,v);
        }
    }

    public static LongDoubleMapWritable read(DataInput in) throws IOException 
    {
    	LongDoubleMapWritable result = new LongDoubleMapWritable();
        result.readFields(in);
        return result;
    }
}