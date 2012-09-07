package org.openflamingo.hadoop.etl;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.mapreduce.FrontDriver;
import org.openflamingo.hadoop.util.HdfsUtils;

import java.io.IOException;

/**
 * 이 클래스는 새로운 Job를 생성하여 수행한다. 새롭게 JOb를 생성하여
 * 설정된 <code>Mapper</code>를 수행한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ProcessJob {
	/** success는 Job 수행 결과 값 */
	private boolean success = false;
	/** cmd는 Job생성에 필요한 정보 */
	private CommandLine cmd;
	/** conf는 Job생성에 필요한 정보*/
	private Configuration conf;
	/** 새롭게 생성된 Job*/
	private Job job;
	/** mappperClass는 새롭게 생된 Job에서 수행 할 Mapper */
	private Class mapperClass;
    private String input;
    private String output;

    /**
	 *
	 * @param cmd 설정 할 CommandLine
	 * @param conf 설정 할 Configuration
	 */
	public ProcessJob(CommandLine cmd, Configuration conf) {
		this.cmd = cmd;
		this.conf = conf;
        this.input = cmd.getOptionValue("input");
        this.output = cmd.getOptionValue("output");
	}
    public ProcessJob(CommandLine cmd, Configuration conf, String input, String output) {
    	this.cmd = cmd;
        this.conf = conf;
        this.input = input;
        this.output = output;
    }
    public ProcessJob(CommandLine cmd, Configuration conf,  String output) {
    	this.cmd = cmd;
        this.conf = conf;
        this.input = cmd.getOptionValue("input");
        this.output = output;
    }

	/**
	 * 정상적으로 수행 했는지 확인한다. 정상적으로 수행 되면 true가 반환되고
	 * 실패 할 경우에는 false가 반환된다.
	 * @return true
	 */
	public boolean is() {
		return success;
	}

	/**
	 * 새로운 {@link Job}를 생성하여
	 * @param maapperClass 수행 할 {@code Mapper}클래스
	 * @return 수행 완료된 {@link ProcessJob}
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public ProcessJob invoke(Class maapperClass, Class reduceClass) throws IOException, InterruptedException, ClassNotFoundException {
		this.mapperClass = maapperClass;
		job = new Job(conf);
		job.setJarByClass(FrontDriver.class);

        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(maapperClass);
        job.setReducerClass(reduceClass);

		job.setNumReduceTasks(1);
        job.getConfiguration().set("delimiter",cmd.getOptionValue("delimiter"));
		job.waitForCompletion(true);
		boolean success = job.waitForCompletion(true);
		if (!success) {
			this.success = false;
			return null;
		}
		this.success = true;
//		removeTempOuputFile(cmd.getOptionValue("output"));
		return this;
	}


	/**
	 * 임시적으로 생성된 {@code File}를 제거한다.
	 * @param filePath 삭제 할 파일 위치
	 * @throws IOException
	 */
	private void removeTempOuputFile(String filePath) throws IOException {
		if (HdfsUtils.isExist(filePath)) {
			HdfsUtils.deleteFromHdfs(filePath);
		}
	}

	/**
	 * 생성된 {@link CounterGroup}를 반환한다.
	 * @return CounterGroup
	 * @throws IOException
	 */
	public CounterGroup getCounterGroup() throws IOException {
		Counters counters =job.getCounters();
		return counters.getGroup(mapperClass.getName());
	}

	/**
	 * 생성된 {@link Counters}를 반환한다.
	 * @return Counters
	 * @throws IOException
	 */
	public Counters getCounters() throws IOException {
		return job.getCounters();
	}
}