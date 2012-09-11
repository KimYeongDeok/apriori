package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.etl.ProcessJob;
import org.openflamingo.hadoop.mapreduce.ETLDriver;
import org.openflamingo.hadoop.repository.AprioriRepositoryMySQL;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriDriver implements ETLDriver {
    private static final Log LOG = LogFactory.getLog(AprioriDriver.class);

    @Override
    public int service(Job job, CommandLine cmd, Configuration conf) throws Exception {
        movieLensMapReduce(job, cmd);

//        long count = sortAndCountMapper(job, cmd);
//        if (count == 0)
//            return (int) count;
//
//        saveTotalSize(count);
//
//        int level = Integer.valueOf(cmd.getOptionValue("level", "0"));
//        conf.setInt("support", 2);
//
//        firstAprioriMapReduce(cmd, conf);
//        othersAprioriMapReduece(cmd, conf, level);

        return 1;
    }


    private void saveTotalSize(long count) {
        AprioriRepositoryMySQL repository = new AprioriRepositoryMySQL();
        try {
            repository.saveTotalSize(count);
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    private void othersAprioriMapReduece(CommandLine cmd, Configuration conf, int level) throws IOException, InterruptedException, ClassNotFoundException {
        String input = cmd.getOptionValue("output") + "1/part*";
        for (int i = 2; i <= level; i++) {
            conf.setInt("level", i);
            String output = cmd.getOptionValue("output") + i;

            ProcessJob processJob = new ProcessJob(cmd, conf, input, output);
            processJob.invoke(AprioriMapper.class, AprioriReduce.class);

            input = output + "/part*";
        }
    }

    private void firstAprioriMapReduce(CommandLine cmd, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        String input = cmd.getOptionValue("output");
        String output = input + "1";
        ProcessJob firstMapperJob = new ProcessJob(cmd, conf, input, output);
        firstMapperJob.invoke(AprioriFirstMapper.class, AprioriReduce.class);
    }

    private long sortAndCountMapper(Job job, CommandLine cmd) throws IOException, InterruptedException, ClassNotFoundException {
        int result;
        // Mapper Class
        job.setMapperClass(AprioriSortMapper.class);

        // Output Key/Value
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));
        job.setNumReduceTasks(0);

        result = job.waitForCompletion(true) ? 1 : 0;

        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(AprioriSortMapper.COUNTER.TOTAL_COUNT);

        return counter.getValue();
    }
    private int movieLensMapReduce(Job job, CommandLine cmd) throws IOException, InterruptedException, ClassNotFoundException {
         int result;
         // Mapper Reduce Class
         job.setMapperClass(MovieLensMapper.class);
         job.setReducerClass(MovieLensReduce.class);

         // Output Key/Value
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(Text.class);

         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);

         job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));
         job.setNumReduceTasks(1);

//        FileInputFormat.addInputPaths(job, cmd.getOptionValue("input"));
//        FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("output")));

         return job.waitForCompletion(true) ? 1 : 0;
     }
}
