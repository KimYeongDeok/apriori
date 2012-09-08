package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
public class AprioriDataBaseMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(AprioriDataBaseMapper.class);
    private String delimiter;
    private int level;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        delimiter = configuration.get("delimiter");
        level = configuration.getInt("level", 0);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String tempRow = value.toString();
        int indexSpace = tempRow.indexOf(" ");
        int indexDelimeter = tempRow.indexOf(delimiter);
        int length = Math.abs(indexSpace - indexDelimeter);

        String firstRow = null;
        String secondRow = null;

        if (indexSpace < 0 || length <= 1) {
            indexSpace = 1;
            firstRow = tempRow.substring(0, indexSpace);
            secondRow = tempRow.substring(indexSpace + 1, tempRow.length());
        }
        if (indexSpace >= 0 || length >= 1) {
            firstRow = tempRow.substring(0, indexSpace);
            secondRow = tempRow.substring(indexSpace + 1, tempRow.length());
        }

        LOG.info("+++++++++++++++++++++++");
        LOG.info(tempRow);
        LOG.info(firstRow+"    "+secondRow);
        LOG.info(indexSpace+"     "+indexDelimeter);

        StringTokenizer stringTokenizer = new StringTokenizer(secondRow, delimiter);
        List<String> stringValues = toList(stringTokenizer);
        int valueSize = stringValues.size();

        if (level > valueSize)
            return;

        for (int i = 0; i < valueSize; i++) {
            if (1 >= stringValues.size()) {
                String rowKey = createKey(firstRow, stringValues);
                context.write(new Text(rowKey), new Text("NULL"));
                return;
            }

            String rowKey = createKey(firstRow, stringValues);
            String rowValue = createValue(stringValues);
            context.write(new Text(rowKey), new Text(rowValue));
        }
    }


    private String createKey(String key, List<String> stringValues) {
        key = key + delimiter + stringValues.get(0);
        stringValues.remove(0);
        return key;
    }

    private String createValue(List<String> stringValues) {
        StringBuilder builder = new StringBuilder();
        for (String stringValue : stringValues) {
            builder.append(stringValue).append(delimiter);
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
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
