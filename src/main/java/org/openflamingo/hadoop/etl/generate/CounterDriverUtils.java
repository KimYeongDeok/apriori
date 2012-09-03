package org.openflamingo.hadoop.etl.generate;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.openflamingo.hadoop.etl.ProcessJob;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CounterDriverUtils {
	public static CounterGroup countJobMapper(CommandLine cmd, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		ProcessJob processJob = new ProcessJob(cmd, conf).invoke(GenerateCountMapper.class);
		if (!processJob.is())
			throw new InterruptedException("Counter Job runs failed.");
		return processJob.getCounterGroup();
	}

	public static void settingCounterToMapper(Job job, CounterGroup counters) {
		long key = 0;
		for (Counter counter : counters) {
			job.getConfiguration().setLong(counter.getName(), key);
			key += counter.getValue();
		}
	}
}
