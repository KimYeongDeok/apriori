package org.openflamingo.hadoop.mapreduce.grep;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.mapreduce.ETLDriver;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GrepDriver implements ETLDriver {

	@Override
	public int service(Job job,CommandLine cmd, Configuration conf) throws Exception {
		// Mapper Class
		job.setMapperClass(GrepMapper.class);

		// Output Key/Value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("grep", cmd.getOptionValue("parameter"));
		//job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));

		// Reducer Task
		job.setNumReduceTasks(0);
		return 0;
	}
}
