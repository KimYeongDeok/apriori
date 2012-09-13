package org.openflamingo.hadoop.commons.generate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 이 클래스는 {@link org.openflamingo.hadoop.commons.job.ProcessJob}에서 사용되는 카운터 {@code
 * Mapper}이다. {@code ProcessJob}에서 사용되는 {@code Mapper}이며 {@code Mapper}별로
 * 카운터 해서 총 개수를 {@link org.apache.hadoop.mapreduce.Counter}에 클래스 이르으로 저장
 * 한다.
 *
 * <pre>
 *     Counters counters =job.getCounters();
 *	   counters.getGroup(GenerateCountMapper.class.getName());
 * </pre>
 *
 * @see Mapper
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GenerateCountMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String taskID;

	public static enum COUNTER{
		TOTAL_COUNT
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		taskID = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(this.getClass().getName(), taskID).increment(1);
		context.getCounter(COUNTER.TOTAL_COUNT).increment(1);
	}
}
