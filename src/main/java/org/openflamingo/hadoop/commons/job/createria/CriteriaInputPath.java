package org.openflamingo.hadoop.commons.job.createria;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CriteriaInputPath extends CriteriaJob<String> {
    public CriteriaInputPath(String parameter) {
        super(parameter);
    }

    @Override
    public void setting(Job job) throws IOException {
        FileInputFormat.addInputPaths(job, parameter);
    }
}
