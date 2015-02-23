package Mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StockMapper1 extends Mapper<Object, Text, Text, Text> {

	private Text name = new Text();
	private Text datePrice = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] stockData = value.toString().split(",");
		if(!stockData[0].equals("Date"))
		{
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			String companyKey = fileName.split("-")[0].replace(".csv", "");		
			String[] dateValues = stockData[0].split("-");
			String dt1 = dateValues[0];
			String dt2 = dateValues[1];
			companyKey = companyKey+"-"+dt1 + "-"+dt2;
			name.set(companyKey);
			datePrice.set(dateValues[2] + " " + stockData[6]);
			context.write(name, datePrice);
		}
	}

}
