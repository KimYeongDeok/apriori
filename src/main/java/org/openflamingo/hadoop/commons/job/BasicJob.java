package org.openflamingo.hadoop.commons.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.openflamingo.hadoop.commons.job.createria.Criteria;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class BasicJob {
    private Job job;
    public BasicJob(Class jarByClass, Configuration conf) throws IOException {
    }

    public BasicJob add(Criteria criteria) throws IOException {
        criteria.setting(job);
        return this;
    }
}