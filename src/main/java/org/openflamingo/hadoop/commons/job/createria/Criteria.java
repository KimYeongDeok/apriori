package org.openflamingo.hadoop.commons.job.createria;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public interface Criteria {
    public void setting(Job job) throws IOException;
}
