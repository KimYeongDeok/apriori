package org.openflamingo.hadoop.mapreduce.generate;

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
public class GenerateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String delimiter;
	private long startKeyPosition;
	private String generate;
	private String date;
	private String simpleName;
	private String t;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		delimiter = configuration.get("delimiter");
		generate = configuration.get("generate");
		date = configuration.get("date");

		String simpleName = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
		startKeyPosition = configuration.getLong(simpleName, 0);

		super.setup(context);
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();

		builder.append(value.toString())
			   .append(delimiter);

		if(generate.equals("sequence"))
			builder.append(startKeyPosition++);
		else
			builder.append(date);

		context.write(NullWritable.get(), new Text(builder.toString()));
	}
}

