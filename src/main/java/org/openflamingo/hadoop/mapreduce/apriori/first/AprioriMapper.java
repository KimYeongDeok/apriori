package org.openflamingo.hadoop.mapreduce.apriori.first;

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
public class AprioriMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(AprioriMapper.class);
    private String delimiter;
    private int number;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        delimiter = configuration.get("delimiter");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String tempRow = value.toString();
        int indexSpace = tempRow.indexOf("\t");

        String firstRow = tempRow.substring(0, indexSpace);
        String secondRow = tempRow.substring(indexSpace + 1, tempRow.length());

        StringTokenizer stringTokenizer = new StringTokenizer(secondRow, delimiter);
        List<String> stringValues = toList(stringTokenizer);
        int valueSize = stringValues.size();

        while (1 != stringValues.size()) {
            String rowKey = createKey(firstRow, stringValues);
            String rowValue = createValue(stringValues);
            context.write(new Text(rowKey), new Text(rowValue));
        }
        String rowKey = createKey(firstRow, stringValues);
        context.write(new Text(rowKey), new Text("NULL"));
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
        builder.delete(builder.length() - delimiter.length(), builder.length());
        return builder.toString();
    }

    private List<String> toList(StringTokenizer stringTokenizer) {
        List<String> list = new ArrayList<String>();

//        String first = generateObject(stringTokenizer, list);
//        if (first == null)
//            return null;

        while (stringTokenizer.hasMoreTokens()) {
            String string = stringTokenizer.nextToken();
            if (!"".equals(string.trim()))
                list.add(string);
        }
        return list;
    }

    private String generateObject(StringTokenizer stringTokenizer, List<String> list) {
        if (!stringTokenizer.hasMoreTokens())
            return null;
        String terms = stringTokenizer.nextToken();
        int index = terms.indexOf("/N/");
        if (index != 0) {
            return terms;
        }
        String[] row = terms.split("/N/");
        number = Integer.valueOf(row[1]);

        return row[2];
    }
}
