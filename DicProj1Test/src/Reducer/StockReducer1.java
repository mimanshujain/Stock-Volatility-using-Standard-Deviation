package Reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer1 extends Reducer<Text, Text, Text, Text>{

//	int firstDay = 0, lastDay = 0;
	String firstPrice = "";
	String lastPrice = "";
//	firstDay == 0 && lastDay == 0 && 
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {			
		if(lastPrice.equals("") && firstPrice.equals(""))
		{
			Iterator<Text> it = values.iterator();
			String lastData =  "", firstData = "";
			Text trans = null;
			
			while(it.hasNext())
			{
				if(lastData.equals(""))
					lastData = it.next().toString();
				else
					trans = it.next();
			}
			
			firstData = trans.toString();
			lastPrice = lastData.split(" ")[1];
			firstPrice = firstData.split(" ")[1];

//			for(Text value : values)
//			{
//				String[] datePrice = value.toString().split(" ");
//				int day = Integer.parseInt(datePrice[0]);
//				String price = datePrice[1];
//				setRange(day, price);
//			}
			double first = Double.parseDouble(firstPrice);
			double last = Double.parseDouble(lastPrice);
			Double diff = (last - first)/first;
			DoubleWritable priceDiff = new DoubleWritable(diff);
			Text dummy = new Text(priceDiff+"");
			//		reduceVal.set(priceDiff.toString());
			context.write(key, dummy);		

//			firstDay = 0;
//			lastDay = 0;
			firstPrice = "";
			lastPrice = "";
		}
	}

//	private void setRange(int day, String price)
//	{
//		if(firstDay == 0 && lastDay == 0)
//		{
//			firstDay = day;
//			lastDay = day;
//			firstPrice = price;
//			lastPrice = price;
//		}
//
//		else
//		{
//			if(day < firstDay)
//			{
//				firstDay = day;
//				firstPrice = price;
//				return;
//			}
//			if(day > lastDay)
//			{
//				lastDay = day;
//				lastPrice = price;
//				return;
//			}
//		}
//	}	

}
