package org.openflamingo.hadoop.commons.job.createria;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CriteriaMapper extends CriteriaJob<Class> {
    public CriteriaMapper(Class parameter) {
        super(parameter);
    }

    @Override
    public void setting(Job job) throws IOException {
        job.setMapperClass(parameter);
    }
}
