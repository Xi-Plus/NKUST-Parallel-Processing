package HBaseIA.TwitBase.mapreduce;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import HBaseIA.TwitBase.hbase.TwitsDAO;

public class CountShakespeare {

	// Extend the base Mapper class to add the required input key and value classes
	// public abstract class TableMapper<KEYOUT,VALUEOUT>
	public static class Map extends TableMapper<Text, LongWritable> {

		public static enum Counters {
			ROWS, SHAKESPEAREAN
		};

		/**
		 * Determines if the message pertains to Shakespeare.
		 */
		private boolean containsShakespear(String msg) {
			// Return the next pseudorandom, uniformly distributed boolean value from this
			// random number generator's sequence
			return msg.contains("@AppleSupport");
		}

		// A byte sequence that is usable as a key or value
		// Based on BytesWritable
		// this class is NOT resizable and DOES NOT distinguish between the size of the
		// sequence and the current capacity as BytesWritable does
		@Override
		protected void map(ImmutableBytesWritable rowkey, Result result, Context context) {
			// The Cell for the most recent timestamp for a given column
			// Return value in a new byte array
			byte[] b = result.getColumnLatest(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL).getValue();
			if (b == null)
				return;

			String msg = Bytes.toString(b);
			if (msg.isEmpty())
				return;

			// A named counter that tracks the progress of a map & reduce job
			// Get the Counter for the given counterName
			// Increment this counter by the given value
			context.getCounter(Counters.ROWS).increment(1);
			// Get the Counter for the given counterName
			// Get the current value of this counter
			System.out.println("Row " + context.getCounter(Counters.ROWS).getValue());
			if (containsShakespear(msg)) {
				context.getCounter(Counters.SHAKESPEAREAN).increment(1);
				System.out.println("@AppleSupport " + context.getCounter(Counters.SHAKESPEAREAN).getValue());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// create a org.apache.hadoop.conf.Configuration with HBase resources
		Configuration conf = HBaseConfiguration.create();
		// conf.set("mapreduce.job.jar", "/home/xiplus/midterm/HBaseIA.jar");

		Job job = new Job(conf, "TwitBase Shakespeare counter");
		// set the Jar by finding where a given class came from
		job.setJarByClass(CountShakespeare.class);
		job.setJar("HBaseIA.jar");

		// job.setMapperClass(Map.class);

		// Used to perform Scan operations
		Scan scan = new Scan();
		// Get the column from the specified family with the specified qualifier
		scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
		// Utility for TableMapper and TableReducer
		// set up the TableMap job
		// table - Binary representation of the table name to read from
		// scan - The scan instance with the columns
		// mapper - The mapper class to use
		// outputKeyClass - The class of the output key; here is the row key
		// outputValueClass - The class of the output value; here is the scan result
		// job - The current job to adjust
		TableMapReduceUtil.initTableMapperJob(Bytes.toString(TwitsDAO.TABLE_NAME), scan, Map.class,
				ImmutableBytesWritable.class, Result.class, job);

		// set the OutputFormat for the job
		// job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(FileOutputFormat.class);
		// job.setOutputFormatClass(NullOutputFormat.class);

		// FileInputFormat.setInputPaths(job, new Path("/home/xiplus/cs/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/xiplus/cs/output"));
		// job.waitForCompletion(true);
		// FileInputFormat.setOutputPath(job, new Path("/home/xiplus/cs"));
		// FileOutputFormat.setOutputPath(job, new Path("/home/xiplus/cs"));
		// set the number of reduce tasks for the job
		job.setNumReduceTasks(0);
		// submit the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
