package org.openflamingo.hadoop.commons.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.commons.job.createria.Criteria;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class TextInputOutputJob {
    private Job job;

    public TextInputOutputJob(Class jarByClass, Configuration conf) throws IOException {
        this.job = new Job(conf);
        this.job.setJarByClass(jarByClass);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
    }

    public TextInputOutputJob add(Criteria criteria) throws IOException {
        criteria.setting(job);
        return this;
    }

    public int start() throws ClassNotFoundException, IOException, InterruptedException {
        return job.waitForCompletion(true) ? 1 : 0;
    }

    public Job getJob(){
        return job;
    }

    public long getCount(Enum<?> key) throws IOException {
        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(key);
        return counter.getValue();
    }
}
