package org.openflamingo.hadoop.mapreduce.apriori;

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
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), delimiter);

        String resultKey = createKey(stringTokenizer);
        String resultValue = createValue(stringTokenizer);

        context.write(new Text(resultKey), new Text(resultValue));
    }

    private String createKey(StringTokenizer stringTokenizer) {
        List<String> strings = new ArrayList<String>();
        for (int i = 0; i < level; i++) {
            if (stringTokenizer.hasMoreTokens()) {
                String string = stringTokenizer.nextToken();
                if (!"".equals(string.trim()))
                    strings.add(string);
            }
        }
        return toString(strings);
    }

    private String createValue(StringTokenizer stringTokenizer) {
        List<String> strings = new ArrayList<String>();
        while (stringTokenizer.hasMoreTokens()) {
            String string = stringTokenizer.nextToken();
            if (!"".equals(string.trim()))
                strings.add(string);
        }
        return toString(strings);
    }

    private String toString(List<String> strings) {
        if (strings.size() == 0)
            return "";
        StringBuilder builder = new StringBuilder();
        for (String string : strings) {
            builder.append(string).append(delimiter);
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }
}
