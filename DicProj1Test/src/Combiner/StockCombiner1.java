package Combiner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockCombiner1 extends Reducer<Text, Text, Text, Text>{

	int lastDay = 0, firstDay = 0;
	String lastPrice = "";
	String firstPrice = "";

	private Text reduceVal = new Text();

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {		
		if(firstDay == 0 && lastDay == 0 && lastPrice.equals("") && firstPrice.equals(""))
		{
//			Iterator<Text> it = values.iterator();
//			String lastData =  "", firstData = "";
//			Text trans = null;
//			while(it.hasNext())
//			{
//				if(lastData.equals(""))
//					lastData = it.next().toString();
//				else
//					trans = it.next();
//			}
//			firstData = trans.toString();
//			lastPrice = lastData.split(" ")[1];
//			String lDay = lastData.split(" ")[0];
//			firstPrice = firstData.split(" ")[1];
//			String fDay = firstData.split(" ")[0];

			for(Text value : values)
			{
				String[] datePrice = value.toString().split(" ");
				int day = Integer.parseInt(datePrice[0]);
				String price = datePrice[1];
				setRange(day, price);
			}
			reduceVal.set(lastDay + " " + lastPrice);
			context.write(key, reduceVal);
			reduceVal.set(firstDay + " " + firstPrice);
			context.write(key, reduceVal);
			
			firstDay = 0;
			lastDay = 0;
			firstPrice = "";
			lastPrice = "";
		}		
	}

	private void setRange(int day, String price)
	{
		if(lastDay == 0 && firstDay == 0)
		{
			lastDay = day;
			firstDay = day;
			lastPrice = price;
			firstPrice = price;
		}
		else
		{
			if(day > lastDay)
			{
				lastDay = day;
				lastPrice = price;
				return;
			}
			if(day < firstDay)
			{
				firstDay = day;
				firstPrice = price;
				return;
			}
		}
	}	

}
