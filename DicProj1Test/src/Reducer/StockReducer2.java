package Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer2 extends Reducer<Text, DoubleWritable, Text, Text>{
	double sum = 0D;
	double diff = 0D;
	ArrayList<Double> valueList;
	
	public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {	
		int counter = 0;
		sum = 0D;
		valueList = new ArrayList<Double>();
		ArrayList<Double> diffList = new ArrayList<Double>();
//		Iterator<DoubleWritable> it = values.iterator();
		
		for(DoubleWritable value : values)
		{
			valueList.add(value.get());
			sum = sum + value.get();
			counter++;
		}
//		while(it.hasNext())
//		{
//			sum += it.next().get();
//			counter++;
//		}
		
		sum = sum/counter;
		int c = 0;//remove it
		
		Iterator<Double> it = valueList.iterator();
		
		while(it.hasNext())
		{
			diff = 0D;
			diff = it.next() - sum;
			diffList.add(Math.pow(diff, 2.0));
			c++;
		}
		
//		for(DoubleWritable value : values)
//		{
//			diff = 0D;
//			diff = value.get() - sum;
//			diffList.add(Math.pow(diff, 2.0));
//			c++;
//		}
		
		Iterator<Double> it2 = diffList.iterator();
		double addDiff = 0D;
		
		while(it2.hasNext())
			addDiff = addDiff + it2.next();
		
		addDiff = addDiff/(counter - 1);
		DoubleWritable volatility = new DoubleWritable(Math.sqrt(addDiff));
		Text dummy = new Text(volatility+""); //+" "+sum+" "+addDiff+" "+counter+" "+c	
		context.write(dummy, key);
		valueList = null;
	}
	
}
