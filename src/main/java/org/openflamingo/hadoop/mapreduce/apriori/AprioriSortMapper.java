package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriSortMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private String delimiter;
    public static enum COUNTER{
   		TOTAL_COUNT
   	}

    @Override
   	protected void setup(Context context) throws IOException, InterruptedException {
   		Configuration configuration = context.getConfiguration();
        delimiter = configuration.get("delimiter");
   	}
    @Override
   	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String result = toSortString(value);

        context.write(NullWritable.get(), new Text(result));

        context.getCounter(COUNTER.TOTAL_COUNT).increment(1);
   	}

    private String toSortString(Text value) {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), delimiter);
        List<String> strings = new ArrayList<String>();
        while(stringTokenizer.hasMoreTokens()){
            String string = stringTokenizer.nextToken();
            if( !"".equals(string.trim()) )
                strings.add(string);
        }
        Collections.sort(strings);

        StringBuilder builder = new StringBuilder();
        for (String string : strings) {
            builder.append(string).append(delimiter);
        }
        builder.delete(builder.length()-1, builder.length());
        return builder.toString();
    }
}
