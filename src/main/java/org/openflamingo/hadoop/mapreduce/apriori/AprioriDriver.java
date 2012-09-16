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
import org.openflamingo.hadoop.commons.MapReduceConfigration;
import org.openflamingo.hadoop.commons.job.ProcessJob;
import org.openflamingo.hadoop.commons.job.TextInputOutputJob;
import org.openflamingo.hadoop.commons.job.createria.*;
import org.openflamingo.hadoop.commons.message.ErrorMessage;
import org.openflamingo.hadoop.commons.sequence.SequencFilePath;
import org.openflamingo.hadoop.mapreduce.ETLDriver;
import org.openflamingo.hadoop.mapreduce.FrontDriver;
import org.openflamingo.hadoop.mapreduce.apriori.first.AprioriFirstMapper;
import org.openflamingo.hadoop.mapreduce.apriori.first.AprioriMapper;
import org.openflamingo.hadoop.mapreduce.apriori.first.AprioriReduce;
import org.openflamingo.hadoop.mapreduce.apriori.first.AprioriSortMapper;
import org.openflamingo.hadoop.mapreduce.apriori.second.SupportMapper;
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
	private SequencFilePath supportFile;
    @Override
    public int service(Job job, CommandLine cmd, Configuration conf) throws Exception {
		this.supportFile = new SequencFilePath(cmd.getOptionValue("output"));
        int level = Integer.valueOf(cmd.getOptionValue("level", "0"));
        conf.set("delimiter", cmd.getOptionValue("delimiter"));
        conf.setInt("level", level);
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

        LOG.info("===============Send TotalSize(" + count + ") To DataBase Start===============");
        saveTotalSize(count);
        LOG.info("===============Send TotalSize To DataBase complete===============");

        LOG.info("===============Aprior First Support MapReduce Start===============");
        String firstMapReduceOuput = firstAprioriMapReduce(cmd, conf);
        LOG.info("===============Aprior First Support MapReduce Complete===============");

        LOG.info("===============Aprior Support MapReduce Start===============");
        othersAprioriMapReduece(cmd, conf, level);
        LOG.info("===============Aprior Support MapReduce Complete===============");

	    LOG.info("===============Aprior Confidence/lift MapReduce Start===============");
	    ConfidenceAndLiftMapReduce(cmd, conf, firstMapReduceOuput, count);
	    LOG.info("===============Aprior Confidence/lift MapReduce Complete===============");
        return 1;
    }

	private int ConfidenceAndLiftMapReduce(CommandLine cmd, Configuration conf, String firstMapReduceOuput, long count) throws IOException, ClassNotFoundException, InterruptedException {
		conf.setLong("totalSize",count);
		String input = firstMapReduceOuput;
		String output = cmd.getOptionValue("output")+"result";
		TextInputOutputJob job = new TextInputOutputJob(FrontDriver.class, conf)
		               .add(new CriteriaMapper(SupportMapper.class))
		               .add(new CriteriaInputPath(input))
		               .add(new CriteriaOutputPath(output))
		               .add(new CriteriaNumReduceTasks(0));
		int success = job.start();
		if (success == ErrorMessage.Fail)
		    return 0;
		return 1;
	}

	private void dropDataBase() {
        ArrayList<MySQLConnector> list = new ArrayList<MySQLConnector>();
        list.add(new MySQLConnector(MapReduceConfigration.URL));

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

        job.setNumReduceTasks(MapReduceConfigration.NUMBER_REDUCE_TASK);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    private long sortAndCountMapper(Configuration conf, CommandLine cmd) throws IOException, InterruptedException, ClassNotFoundException {
        String input = cmd.getOptionValue("input");
        String sortedOutput = supportFile.next();

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

    private String firstAprioriMapReduce(CommandLine cmd, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        String input = supportFile.current();
        String output = supportFile.next();

        ProcessJob firstMapperJob = new ProcessJob(cmd, conf, input, output);
        firstMapperJob.start(AprioriFirstMapper.class, AprioriReduce.class);
	    return output;
    }


    private void othersAprioriMapReduece(CommandLine cmd, Configuration conf, int level) throws IOException, InterruptedException, ClassNotFoundException {
        String input = supportFile.currentMapReduceFile();//cmd.getOptionValue("output") + "1/part*";
        for (int i = 2; i <= level; i++) {
            conf.setInt("level", i);

            ProcessJob processJob = new ProcessJob(cmd, conf, input, supportFile.next());
            processJob.start(AprioriMapper.class, AprioriReduce.class);
            input = supportFile.currentMapReduceFile();
        }
    }
}
