package org.openflamingo.hadoop.commons.job.createria;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CriteriaOutputPath extends CriteriaJob<String> {
    public CriteriaOutputPath(String parameter) {
        super(parameter);
    }

    @Override
    public void setting(Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(parameter));
    }
}
