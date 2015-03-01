package Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer1 extends Reducer<Text, Text, Text, Text>{

	int firstDay = 0, lastDay = 0;
	String firstPrice = "";
	String lastPrice = "";
	private static Text countText= new Text("-1");
	private static int count = 0;
	//	firstDay == 0 && lastDay == 0 && 

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {		
		
		HashMap<String, TreeMap<String, String>> map = new HashMap<String, TreeMap<String,String>>();

		Iterator<Text> it = values.iterator();
		String[] data;
		String name = "";
		String day = "";
		String adjClose = "";
		ArrayList<Double> diffList = new ArrayList<Double>();
		TreeMap<String, String> tree = null;
		while(it.hasNext())
		{
			data = it.next().toString().split(" ");
			name = key.toString()+" "+data[2]+" "+data[3];
			day = data[0];
			adjClose = data[1];
			if(map.containsKey(name))
			{
				tree = map.get(name);
				tree.put(day, adjClose);
			}
			else
			{
				tree = new TreeMap<String, String>();
				tree.put(day, adjClose);
				map.put(name, tree);
			}
			tree = null;
		}
		tree = null;
		Iterator<String> it2 = map.keySet().iterator();
		double first= 0D;
		double last = 0D;
		double diff = 0D;
		while(it2.hasNext())
		{
			tree = map.get(it2.next());
			firstPrice = tree.get(tree.firstKey());
			lastPrice = tree.get(tree.lastKey());

			first = Double.parseDouble(firstPrice);
			last = Double.parseDouble(lastPrice);
			diff = (last - first)/first;
			diffList.add(diff);
			tree = null;
		}
		double sum = 0D;
		int counter = 0;
		Iterator<Double> it3 = diffList.iterator();
		while(it3.hasNext())
		{
			sum = sum + it3.next();
			counter++;
		}
		sum = sum/counter;

		it3 = diffList.iterator();
		double addDiff = 0D;
		while(it3.hasNext())
		{
			diff = 0D;
			diff = it3.next() - sum;
			diff = Math.pow(diff, 2.0);
			addDiff = addDiff + diff;
		}

		if(counter > 1)
			addDiff = addDiff/(counter - 1);
		if(addDiff != 0.0)
		{
			Text dummy = new Text(Math.sqrt(addDiff)+"");
			context.write(dummy, key);
			firstPrice = "";
			lastPrice = "";
		}
	}

}
