package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.commons.job.ProcessJob;
import org.openflamingo.hadoop.commons.job.TextInputOutputJob;
import org.openflamingo.hadoop.commons.job.createria.*;
import org.openflamingo.hadoop.commons.message.ErrorMessage;
import org.openflamingo.hadoop.mapreduce.ETLDriver;
import org.openflamingo.hadoop.mapreduce.FrontDriver;
import org.openflamingo.hadoop.repository.AprioriRepositoryMySQL;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;

import java.io.IOException;
import java.util.ArrayList;

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
        int level = Integer.valueOf(cmd.getOptionValue("level", "0"));
        conf.set("delimiter", cmd.getOptionValue("delimiter"));
        conf.setInt("support", Integer.valueOf(cmd.getOptionValue("support")));

        LOG.info("===============Drop Table Start===============");
        dropDataBase();
        LOG.info("===============Drop Table Complete===============");

        LOG.info("===============MovieLens Data Start===============");
//        movieLensMapReduce(job, cmd);
        LOG.info("===============MovieLens Data Complete===============");

        long count = sortAndCountMapper(conf, cmd);
        if (count == ErrorMessage.Fail)
            return ErrorMessage.Fail;

        LOG.info("===============Send TotalSize("+count+") To DataBase Start===============");
        saveTotalSize(count);
        LOG.info("===============Send TotalSize To DataBase complete===============");

        LOG.info("===============Aprior First MapReduce Start===============");
        firstAprioriMapReduce(cmd, conf);
        LOG.info("===============Aprior First MapReduce Complete===============");

        LOG.info("===============Aprior MapReduce Start===============");
        othersAprioriMapReduece(cmd, conf, level);
        LOG.info("===============Aprior MapReduce Complete===============");
        return 1;
    }

    private void dropDataBase() {
        ArrayList<MySQLConnector> list = new ArrayList<MySQLConnector>();
        list.add(new MySQLConnector("61.43.139.103"));
//        list.add(new MySQLConnector("hadoop3", "yd"));
//        list.add(new MySQLConnector("hadoop4", "yd"));
//        list.add(new MySQLConnector("hadoop5", "yd"));
//        list.add(new MySQLConnector("hadoop6", "yd"));
//        list.add(new MySQLConnector("hadoop7", "yd"));
//        list.add(new MySQLConnector("hadoop8", "yd"));

        for (MySQLConnector mySQLConnector : list) {
            mySQLConnector.dropTable();
        }
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

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    private long sortAndCountMapper(Configuration conf, CommandLine cmd) throws IOException, InterruptedException, ClassNotFoundException {
        String input = cmd.getOptionValue("input");
        String sortedOutput = cmd.getOptionValue("output") + "0";

        TextInputOutputJob job = new TextInputOutputJob(FrontDriver.class, conf)
                .add(new CriteriaMapper(AprioriSortMapper.class))
                .add(new CriteriaInputPath(input))
                .add(new CriteriaOutputPath(sortedOutput))
                .add(new CriteriaMapOutputKeyClass(NullWritable.class))
                .add(new CriteriaNumReduceTasks(0));
        int success = job.start();
        if (success == ErrorMessage.Fail)
            return 0;
        return job.getCount(AprioriSortMapper.COUNTER.TOTAL_COUNT);
    }

    private void saveTotalSize(long count) {
        AprioriRepositoryMySQL repository = new AprioriRepositoryMySQL();
        try {
            repository.saveTotalSize(count);
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    private void firstAprioriMapReduce(CommandLine cmd, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        String input = cmd.getOptionValue("output") + "0";
        String output = cmd.getOptionValue("output") + "1";
        ProcessJob firstMapperJob = new ProcessJob(cmd, conf, input, output);
        firstMapperJob.start(AprioriFirstMapper.class, AprioriReduce.class);
    }


    private void othersAprioriMapReduece(CommandLine cmd, Configuration conf, int level) throws IOException, InterruptedException, ClassNotFoundException {
        String input = cmd.getOptionValue("output") + "1/part*";
        for (int i = 2; i <= level; i++) {
            conf.setInt("level", i);
            String output = cmd.getOptionValue("output") + i;

            ProcessJob processJob = new ProcessJob(cmd, conf, input, output);
            processJob.start(AprioriMapper.class, AprioriReduce.class);

            input = output + "/part*";
        }
    }
}
