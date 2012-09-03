package org.openflamingo.hadoop.mapreduce.group;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
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
public class GroupDriver implements ETLDriver {
	@Override
	public int service(Job job, CommandLine cmd, Configuration conf) throws Exception {
		// Mapper Class
		job.setMapperClass(GroupMapper.class);
		job.setCombinerClass(GroupCombiner.class);
		job.setReducerClass(GroupReducer.class);

		// Output Key/Value
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.getConfiguration().set("group", cmd.getOptionValue("parameter"));
		job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));

		//Reducer Task
		job.setNumReduceTasks(2);
		return 0;
	}
}
