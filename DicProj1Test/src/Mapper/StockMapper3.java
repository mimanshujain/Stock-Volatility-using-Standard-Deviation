package Mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StockMapper3 extends Mapper<Object, Text, DoubleWritable, Text>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] compVal = value.toString().split("\\s+");
		double val = Double.parseDouble(compVal[0]);
		context.write(new DoubleWritable(val), new Text(compVal[1]));
	}
}
