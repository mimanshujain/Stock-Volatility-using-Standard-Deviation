package Mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StockMapper1 extends Mapper<Object, Text, Text, Text> {

	private Text name = new Text();
	private Text datePrice = new Text();

	private static String prevCom = null;
	private static String prevMonth = null;
	private static String prevDay =  null;
	private static String  prevYear = null;
	private static String prevAdjPrice = null;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] stockData = value.toString().split(",");
		
		name.set("");
		datePrice.set("");
		
		if(!stockData[0].equals("Date"))
		{
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			String companyKey = fileName.replace(".csv", "");
			String[] dateValues = stockData[0].split("-");
			String year = dateValues[0];
			String month = dateValues[1];
			String day = dateValues[2];
			String adjClosePr = stockData[6];

			if(StockMapper1.prevCom == null)
			{
				StockMapper1.prevCom = companyKey;
				StockMapper1.prevAdjPrice = adjClosePr;
				StockMapper1.prevDay = day;
				StockMapper1.prevMonth = month;
				StockMapper1.prevYear = year;
				
				name.set(companyKey);
				datePrice.set(day + " " + adjClosePr + " " + year + " " + month);				
				context.write(name, datePrice);	
			}
			else
			{
				if(StockMapper1.prevCom.equals(companyKey) && StockMapper1.prevMonth.equals(month)
						&& StockMapper1.prevYear.equals(year))
				{
					StockMapper1.prevDay = day;
					StockMapper1.prevAdjPrice = adjClosePr;
					return;
				}
				
				else
				{
					name.set(StockMapper1.prevCom);

					datePrice.set(StockMapper1.prevDay + " " + StockMapper1.prevAdjPrice + " "
							+ StockMapper1.prevYear + " " + StockMapper1.prevMonth);
					
					context.write(name, datePrice);
					name.set("");
					datePrice.set("");
					
					StockMapper1.prevCom = companyKey;
					StockMapper1.prevAdjPrice = adjClosePr;
					StockMapper1.prevDay = day;
					StockMapper1.prevMonth = month;
					StockMapper1.prevYear = year;

					name.set(companyKey);
					datePrice.set(day + " " + adjClosePr + " " + year + " " + month);	
					context.write(name, datePrice);	
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		if(StockMapper1.prevCom != null && !StockMapper1.prevCom.equals(""))
		{	
			name.set(StockMapper1.prevCom);
			datePrice.set(StockMapper1.prevDay + " " + StockMapper1.prevAdjPrice + " "
					+ StockMapper1.prevYear + " " + StockMapper1.prevMonth);
			context.write(name, datePrice);
		}

	}

}
