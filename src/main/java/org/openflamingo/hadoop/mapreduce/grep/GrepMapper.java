package org.openflamingo.hadoop.mapreduce.grep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GrepMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String regEx;
	private Pattern pattern;

	protected void setup(Context context) throws IOException, InterruptedException {
	Configuration configuration = context.getConfiguration();
	regEx = configuration.get("grep");
	pattern = Pattern.compile(regEx);
	}

	protected void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		String row = value.toString();
		Matcher matcher = pattern.matcher(row);
		if (matcher.find()) {
			context.write(NullWritable.get(), new Text(value));
		}
	}

}
