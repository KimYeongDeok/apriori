package org.openflamingo.hadoop.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.openflamingo.hadoop.commons.MapReduceObjectStore;

/**
 * 모든 ETL Driver가 실행되기전 기본 설정 한다. 사용자가 입력하는 args를
 * 분석하여 <code>CommandLine</code> 만들어 적절한 ETL Driver를 선택하여
 * {@link Job }, {@link CommandLine }, {@link org.apache.hadoop.conf.Configured}
 * 정보를 전달한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FrontDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new FrontDriver(), args);
        System.exit(res);
    }

    /**
     * 기본적인 Job를 생성하고 Driver를 선택한다. 파라미터로 받은 args내용을 분석하여
     * {@link CommandLine}를 생성하여 {@link Job}를 생성하고 Driver를 수행한다.
     *
     * @param args main메소드로부터 입력받은 args배열
     * @return JavaVirtualMachine 종료 값
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(FrontDriver.class);

        CommandLine cmd = parseArguements(args, job);

        ETLDriver driver = (ETLDriver) MapReduceObjectStore.findClassByName(cmd.getOptionValue("command"));
        assert driver != null;
//		driver.service(job, cmd, getConf());

        // Run a Hadoop Job
        return driver.service(job, cmd, getConf());
    }

    /**
     * 파라미터 args배열을 분석하여 {@link CommandLine}를 생성한다.
     *
     * @param args args main메소드로부터 입력받은 args배열
     * @param job  기본적인 파라미터 값을 설정 할 Job 객체
     * @return 파라미터 분석된 {@link CommandLine}
     * @throws Exception
     */
    private CommandLine parseArguements(String[] args, Job job) throws Exception {
        CommandLine cmd = new CommandInspector().parseCommandLine(args);

        ////////////////////////////////////////
        // 파라미터를 Hadoop Configuration에 추가한다
        ////////////////////////////////////////

        if (cmd.hasOption("input")) {
            FileInputFormat.addInputPaths(job, cmd.getOptionValue("input"));
        }

        if (cmd.hasOption("output")) {
            FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("output")));
        }

        if (cmd.hasOption("delimiter")) {
            job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));
        }
        if (cmd.hasOption("support")) {
            job.getConfiguration().setInt("support", Integer.valueOf(cmd.getOptionValue("support")));
        }

        return cmd;
    }
}