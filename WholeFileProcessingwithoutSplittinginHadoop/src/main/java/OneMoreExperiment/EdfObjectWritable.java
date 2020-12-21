package OneMoreExperiment;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EdfObjectWritable extends GenericWritable{
	
	public EdfObjectWritable() {}
	
	public EdfObjectWritable(Text text) {
		super.set(text);
	}
	
	public EdfObjectWritable(LongWritable longWritable) {
		super.set(longWritable);
	}

	 @Override  
     protected Class<? extends Writable>[] getTypes() {  
		 
         return new Class[] {LongWritable.class, Text.class};
         
}
}
