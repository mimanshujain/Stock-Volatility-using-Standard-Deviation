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
		//		ArrayList<Double> valueList = new ArrayList<Double>();
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
			//			valueList.add();
		}
		//		double addDiff = 0D;
		//		it3 = valueList.iterator();
		//		while(it3.hasNext())
		//			addDiff = addDiff + it3.next();

		if(counter > 1)
			addDiff = addDiff/(counter - 1);
		if(addDiff != 0.0)
		{
			//		DoubleWritable volatility = new DoubleWritable();
			Text dummy = new Text(Math.sqrt(addDiff)+""); //+" "+sum+" "+addDiff+" "+counter+" "+c	Math.sqrt(addDiff)
			context.write(dummy, key);
			firstPrice = "";
			lastPrice = "";
//			StockReducer1.count++;
		}
	}
//
//	@Override
//	protected void cleanup(Context context)
//			throws IOException, InterruptedException {
//		 
//		context.write(countText, new Text(StockReducer1.count+"-" + new Date().getTime()));
//
//	}

	//	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {			
	//		
	////		Iterator<Text> it = values.iterator();
	////		while(it.hasNext())
	////		{
	////			context.write(key, it.next());
	////		}
	//		
	//		if(lastPrice.equals("") && firstPrice.equals(""))
	//		{
	//			Iterator<Text> it = values.iterator();
	////			String lastData =  "", firstData = "";
	////			Text trans = null;
	//			String[] data;
	//			while(it.hasNext())
	//			{
	////				if(firstData.equals(""))
	////					firstData = it.next().toString();
	////				else
	////					trans = it.next();
	//				data = it.next().toString().split(" ");
	//				setRange(Integer.parseInt(data[0]), data[1]);
	//			}
	//			
	////			lastData = trans.toString();
	////			lastPrice = lastData.split(" ")[1];
	////			firstPrice = firstData.split(" ")[1];
	//
	////			for(Text value : values)
	////			{
	////				String[] datePrice = value.toString().split(" ");
	////				int day = Integer.parseInt(datePrice[0]);
	////				String price = datePrice[1];
	////				setRange(day, price);
	////			}
	//			double first = Double.parseDouble(firstPrice);
	//			double last = Double.parseDouble(lastPrice);
	//			Double diff = (last - first)/first;
	//			DoubleWritable priceDiff = new DoubleWritable(diff);
	//			Text dummy = new Text(priceDiff+" " + firstPrice+" "+lastPrice + " " + diff);
	//			//		reduceVal.set(priceDiff.toString());
	//			context.write(key, dummy);		
	//
	//			firstDay = 0;
	//			lastDay = 0;
	//			firstPrice = "";
	//			lastPrice = "";
	//		}
	//	}

	private void setRange(int day, String price)
	{
		if(firstDay == 0 && lastDay == 0)
		{
			firstDay = day;
			lastDay = day;
			firstPrice = price;
			lastPrice = price;
		}

		else
		{
			if(day < firstDay)
			{
				firstDay = day;
				firstPrice = price;
				return;
			}
			if(day > lastDay)
			{
				lastDay = day;
				lastPrice = price;
				return;
			}
		}
	}	

}
