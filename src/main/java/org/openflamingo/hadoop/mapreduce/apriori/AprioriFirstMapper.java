package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openflamingo.hadoop.repository.AprioriRepositoryMySQL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(AprioriFirstMapper.class);
    private String delimiter;
    private AprioriRepositoryMySQL repository;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        delimiter = configuration.get("delimiter");
        repository = new AprioriRepositoryMySQL();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), delimiter);
        List<String> stringValues = toList(stringTokenizer);

        int size = stringValues.size();

        while (1 != stringValues.size()) {
            String rowKey = createKey(stringValues);
            String rowValue = createValue(stringValues);
            context.write(new Text(rowKey), new Text(rowValue));
        }
        context.write(new Text(stringValues.get(0)), new Text("NULL"));
        saveTotalSize(size);
    }

    private void saveTotalSize(int valueSize) {
        try {
            repository.saveTotalSize(valueSize);
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    private String createKey(List<String> stringValues) {
        String rowKey = stringValues.get(0);
        removeFirstValue(stringValues);
        return rowKey;
    }


    private String createValue(List<String> stringValues) {
        StringBuilder builder = new StringBuilder();
        for (String stringValue : stringValues) {
            builder.append(stringValue).append(delimiter);
        }
        builder.delete(builder.length() - 1, builder.length());

        return builder.toString();
    }

    private void removeFirstValue(List<String> stringValues) {
        stringValues.remove(0);
    }

    private List<String> toList(StringTokenizer stringTokenizer) {
        List<String> list = new ArrayList<String>();
        while (stringTokenizer.hasMoreTokens()) {
            String string = stringTokenizer.nextToken();
            if (!"".equals(string.trim()))
                list.add(string);
        }
        return list;
    }
}

