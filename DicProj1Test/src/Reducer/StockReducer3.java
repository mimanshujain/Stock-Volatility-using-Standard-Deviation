package Reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer3 extends Reducer<DoubleWritable,Text , Text, Text>{
	
	public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
		
		for(Text value : values)
		{
			context.write(value, new Text(key+""));
		}
	}

}
