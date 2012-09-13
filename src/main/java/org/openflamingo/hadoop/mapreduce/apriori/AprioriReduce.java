package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.openflamingo.hadoop.repository.AprioriRepositoryMySQL;

import java.io.IOException;
import java.util.*;

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
        LOG.info("Repository : "+repository);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        Set<String> list = new HashSet<String>();
        Map<String, Integer> supportMap = new HashMap<String, Integer>();

        int size = 0;
        while (iterator.hasNext()) {
            String value = iterator.next().toString();
            if(list.contains(value))
                countingValue(supportMap, value);
            else
                list.add(value);
            size++;
        }

        if (size < support)
            return;

        saveSupport(key.toString(), size);
        for (String text : list) {
            if("NULL".equals(text))
                continue;
            context.write(key, new Text(text));

            if(supportMap.containsKey(text))
                saveCandidate(key, text, supportMap.get(text));
            else
                saveCandidate(key, text, 1);
        }
    }

    private void countingValue(Map<String, Integer> supportMap, String value) {
        if(supportMap.containsKey(value))
            supportMap.put(value, supportMap.get(value) + 1);
        else
            supportMap.put(value, 1);
    }

    private void saveCandidate(Text key, String text, int support) {
        try {
            repository.saveCadidate(key.toString(), text, support);
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
