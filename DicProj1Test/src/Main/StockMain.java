package Main;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Mapper.StockMapper1;
import Mapper.StockMapper3;
import Reducer.StockReducer1;
import Reducer.StockReducer3;

public class StockMain {

	public static void main(String[] args) throws Exception {
		
		long startTime = new Date().getTime();		
		
		if(args.length < 2) throw new Exception("Expects two Arguments");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Stock Voltility Job-1");
		System.out.println("\n**********Stock Volatilization_Hadoop-> Start**********\n");
		
		job.setJarByClass(StockMain.class);
		job.setMapperClass(StockMapper1.class);
		job.setReducerClass(StockReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileSystem.get(conf).delete(new Path("Transient_1" + args[1]),true);

	    FileOutputFormat.setOutputPath(job, new Path("Transient_1" + args[1]));	    
	    
	    Job job2 = Job.getInstance(conf, "Stock Voltility Job-2");
	    job2.setJarByClass(StockMain.class);
	    job2.setMapperClass(StockMapper3.class);
	    job2.setReducerClass(StockReducer3.class);
	    job2.setMapOutputKeyClass(DoubleWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job2, new Path("Transient_1" + args[1]));
	    FileSystem.get(conf).delete(new Path(args[1]),true);
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	    job.waitForCompletion(true);
	    boolean status = job2.waitForCompletion(true);
	    
	    if(status)
	    {
	    	long endTime = new Date().getTime();
	    	System.out.println("The time taken by the job = " + (endTime - startTime)/1000 + " seconds");
	    }
	    
	    System.out.println("\n**********Stock Volatilization_Hadoop-> End**********\n");
	}

}
