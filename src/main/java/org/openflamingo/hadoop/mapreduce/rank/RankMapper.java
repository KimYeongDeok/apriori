package org.openflamingo.hadoop.mapreduce.rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Description.z
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class RankMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String delimiter;
	private long startKeyPosition;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		delimiter = configuration.get("delimiter");
		String rank = configuration.get("rank");
		String simpleName = context.getTaskAttemptID().toString().split("_m_")[1];

		startKeyPosition = configuration.getLong(simpleName, 0);
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();

		builder.append(value.toString())
			   .append(delimiter)
			   .append(startKeyPosition++);

		context.write(NullWritable.get(), new Text(builder.toString()));
	}
}

