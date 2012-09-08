package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
        for (Text value : values) {
            context.write(key, value);
        }
//        for (ArrayWritable value : values) {
//            Text[] t = (Text[]) value.get();
//            for (Text text : t) {
//                context.write(key , text);
//            }
//        }

//        Iterator<Text> iterator = values.iterator();
//
//        Set<Text> set = new TreeSet<Text>();
//        int size = 0;
//        while (iterator.hasNext()) {
//            set.add(iterator.next());
//            size++;
//        }
//
//        if (set.size() < support)
//            return;
//        LOG.info("++++++++++++++++++++++++++");
//        LOG.info(set);
//        LOG.info(set.size());
//        context.write(key, new Text(size + ""));
//        for (Text text : set) {
//            if( !"NULL".equals(text))
//                context.write(key, text);
//        }
    }
}
