package Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StockReducer3 extends Reducer<DoubleWritable,Text , Text, Text>{

	static int count = 0;
	static int counter = 0;
	static long time;
	static HashMap<String, Double> map = new HashMap<String, Double>();
	static String keyVal = "";
	
	public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	

		for(Text value : values)
		{
			if(StockReducer3.counter == 0)
				context.write(new Text("\nThe top 10 stocks with the lowest (min) volatility are"), new Text(""));
			
			if(StockReducer3.counter < 10)
				keyVal = keyVal + "\n" + value.toString() + "     " +  key.get();
			else if(StockReducer3.counter == 10)
			{
				context.write(new Text(keyVal), new Text());
				StockReducer3.map.put(value.toString(), key.get());
			}
			
			else if(StockReducer3.counter > 10)
				StockReducer3.map.put(value.toString(), key.get());
				
			StockReducer3.counter++;
		}		
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		ValueComparator val = new ValueComparator(StockReducer3.map);
		TreeMap<String, Double> tree = new TreeMap<String, Double>(val);
		tree.putAll(StockReducer3.map);
		int c = 0;
		Iterator<String> it = tree.keySet().iterator();
		context.write(new Text("\nThe top 10 stocks with the highest (max) volatility are"), new Text(""));
		String str = "";
		while(it.hasNext())
		{
			String key = it.next();
			str = str + "\n" + key + "     " +  map.get(key);
			c++;
			if(c > 9)
				break;
		}
		context.write(new Text(str), new Text());
	}
}

class ValueComparator implements Comparator<String>
{
	HashMap<String, Double> map;
	
	public ValueComparator(HashMap<String, Double> map) {
		this.map = map;
	}
	
	@Override
	public int compare(String o1, String o2) {
		
		if(map.get(o1) >= map.get(o2))
		{
			return -1;
		}
		
		else return 1;
	}
	
}
//context.write(value, new Text(key+""));
