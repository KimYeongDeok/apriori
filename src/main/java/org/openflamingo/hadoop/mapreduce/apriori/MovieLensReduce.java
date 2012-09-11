package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class MovieLensReduce extends Reducer<Text, Text, NullWritable, Text> {
    private static final Log LOG = LogFactory.getLog(MovieLensReduce.class);
    private int support;
    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        support = configuration.getInt("support", 0);
        delimiter = configuration.get("delimiter");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        Map<String, String> map = new HashMap<String, String>();

        while(iterator.hasNext()){
            String string = iterator.next().toString();
            String[] strings = parseToStringArray(string);

            String timeStamp = strings[2];
            String movie = strings[0];

            StringBuilder movies = new StringBuilder();
            if(map.containsKey(timeStamp)){
                movies.append(map.get(timeStamp)).append(delimiter).append(movie);
            }else{
                movies.append(movie);
            }
            map.put(timeStamp, movies.toString());
        }

        Collection<String> movies = map.values();
        for (String movie : movies) {
            context.write(NullWritable.get(), new Text(movie));
        }
    }

    private String[] parseToStringArray(String string) {
        StringTokenizer stringTokenizer = new StringTokenizer(string, delimiter);
        String[] strings = new String[stringTokenizer.countTokens()];
        int index =0;
        while(stringTokenizer.hasMoreTokens()){
            strings[index++] = stringTokenizer.nextToken();
        }
        return strings;
    }
}
