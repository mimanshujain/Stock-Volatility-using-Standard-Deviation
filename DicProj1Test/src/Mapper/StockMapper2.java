package Mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class StockMapper2 extends Mapper<Object, Text, Text, DoubleWritable> {
	
	private Text name = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] compVal = value.toString().split("\\s+");
		
		name.set(compVal[0].split("-")[0]);
//		context.write(key, value);
		DoubleWritable writer = new DoubleWritable(Double.parseDouble(compVal[1]));
		context.write(name, writer);
		
	}

}
