package org.yyw.HadoopEDF.ParallelProcessing;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable{
	
	public DoubleArrayWritable(){

		super(DoubleWritable.class);
	}

public DoubleArrayWritable(double[] outputvalue){
	super(DoubleWritable.class);
	DoubleWritable[] arguments = new DoubleWritable[outputvalue.length];
	for(int i=0; i < outputvalue.length; i++){
		arguments[i]=new DoubleWritable(outputvalue[i]);
	}
	
	set(arguments);
}
public static double[] convert2double(DoubleWritable[] w){
    double[] value=new double[w.length];
    for (int i = 0; i < value.length; i++) {
       value[i]=Double.valueOf(w[i].get());
    }
   return value;
}
public DoubleArrayWritable(DoubleWritable[] outputvalue){
	super(DoubleWritable.class);
	DoubleWritable[] arguments = new DoubleWritable[outputvalue.length];
	for(int i=0; i < outputvalue.length; i++){
		arguments[i]=outputvalue[i];
	}
	
	set(arguments);
}


@Override
public String toString(){

	StringBuffer sb = new StringBuffer();
	sb.append("[");
	for(String s :super.toStrings()){
		sb.append(s).append(",");
	}
	if (sb.length() > 0) {
		sb.replace(sb.length() - 1, sb.length(), "");
	    }
	sb.append("]");
	return sb.toString();
}

}