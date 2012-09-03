package org.openflamingo.hadoop.mapreduce.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class GroupReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {

	private String delimiter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		delimiter = configuration.get("delimiter");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		builder.append(key).append(delimiter);

		Iterator<Text> interator = values.iterator();
		while (interator.hasNext()) {
			String text =  interator.next().toString();
			builder.append(text).append(delimiter);
		}
		builder.delete(builder.length()-1, builder.length());

		context.write(NullWritable.get(), new Text(builder.toString()));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}


}