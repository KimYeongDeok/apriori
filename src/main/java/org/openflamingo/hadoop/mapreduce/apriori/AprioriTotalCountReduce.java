package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.openflamingo.hadoop.repository.AprioriRepositoryMySQL;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriTotalCountReduce extends Reducer<Text, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(AprioriTotalCountReduce.class);
    private AprioriRepositoryMySQL repository;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        repository = new AprioriRepositoryMySQL();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalSize = context.getCounter(AprioriSortMapper.COUNTER.TOTAL_COUNT).getValue();
    }
    private void saveTotalSize(long count) {
        AprioriRepositoryMySQL repository = new AprioriRepositoryMySQL();
        try {
            repository.saveTotalSize(count);
        } catch (Exception e) {
            LOG.error(e);
        }
    }
}
