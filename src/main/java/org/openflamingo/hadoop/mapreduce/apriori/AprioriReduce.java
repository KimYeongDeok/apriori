package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.openflamingo.hadoop.repository.AprioriRepositoryMySQL;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriReduce extends Reducer<Text, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(AprioriReduce.class);
    private int support;
    private AprioriRepositoryMySQL repository;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        support = configuration.getInt("support", 0);
        repository = new AprioriRepositoryMySQL();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        Set<String> list = new HashSet<String>();
        int size = 0;
        while (iterator.hasNext()) {
            list.add(iterator.next().toString());
            size++;
        }

        if (size < support)
            return;

        saveSupport(key.toString(), size);
        for (String text : list) {
            if("NULL".equals(text))
                continue;
            context.write(key, new Text(text));
            saveCandidate(key, text);
        }
    }

    private void saveCandidate(Text key, String text) {
        try {
            repository.saveCadidate(key.toString(), text);
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    private void saveSupport(String key, int size) {
        try {
            repository.saveSupport(key, size);
        } catch (Exception e) {
            LOG.error(e);
        }
    }
}
