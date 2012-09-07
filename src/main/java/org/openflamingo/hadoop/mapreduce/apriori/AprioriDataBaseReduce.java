package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriDataBaseReduce extends Reducer<Text, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(AprioriDataBaseReduce.class);
    private int support;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        support = configuration.getInt("support", 0);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();

        TreeSet<Text> set = new TreeSet<Text>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }

        if (set.size() < support)
            return;

        context.write(key, new Text(set.size() + ""));
        for (Text text : set) {
            if( !"NULL".equals(text))
                context.write(key, text);
        }
    }
}
