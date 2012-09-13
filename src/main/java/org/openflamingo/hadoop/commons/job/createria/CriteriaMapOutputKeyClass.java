package org.openflamingo.hadoop.commons.job.createria;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CriteriaMapOutputKeyClass extends CriteriaJob<Class>{
    public CriteriaMapOutputKeyClass(Class parameter) {
        super(parameter);
    }

    @Override
    public void setting(Job job) throws IOException {
        job.setMapOutputKeyClass(parameter);
    }
}
