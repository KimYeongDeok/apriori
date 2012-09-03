package org.openflamingo.hadoop.mapreduce.clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String delimeter;
	private Clean clean;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		String command = configuration.get("clean");
		delimeter = configuration.get("delimiter");

		clean = CleanFactory.create(command, delimeter);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String result = clean.doClean(value.toString());

		context.write(NullWritable.get(), new Text(result));
	}
}
