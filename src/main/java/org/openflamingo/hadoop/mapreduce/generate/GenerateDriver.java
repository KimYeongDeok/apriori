package org.openflamingo.hadoop.mapreduce.generate;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.etl.ProcessJob;
import org.openflamingo.hadoop.etl.generate.CounterDriverUtils;
import org.openflamingo.hadoop.etl.generate.GenerateCountMapper;
import org.openflamingo.hadoop.mapreduce.ETLDriver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GenerateDriver implements ETLDriver {

	@Override
	public int service(Job job, CommandLine cmd, Configuration conf) throws Exception {
		String generate = cmd.getOptionValue("parameter");

		if ("sequence".equals(generate)) {
			CounterGroup counters = CounterDriverUtils.countJobMapper(cmd, conf);
			CounterDriverUtils.settingCounterToMapper(job, counters);
		}
		if ("date".equals(generate)) {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
			Date date = new Date();
			String formatDate = simpleDateFormat.format(date);
			job.getConfiguration().set("date", formatDate);
		}

		// Mapper Classs
		job.setMapperClass(GenerateMapper.class);

		// Output Key/Value
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.getConfiguration().set("generate", generate);
		job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));

		//Reducer Task
		job.setNumReduceTasks(0);
		return 0;
	}

}
