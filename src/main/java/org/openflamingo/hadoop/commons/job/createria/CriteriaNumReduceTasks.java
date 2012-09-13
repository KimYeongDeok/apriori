package org.openflamingo.hadoop.commons.job.createria;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CriteriaNumReduceTasks extends CriteriaJob<Integer>{
    public CriteriaNumReduceTasks(Integer parameter) {
        super(parameter);
    }

    @Override
    public void setting(Job job) throws IOException {
        job.setNumReduceTasks(parameter);
    }
}
