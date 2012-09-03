package org.openflamingo.hadoop.mapreduce.rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeMap;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class RankReducer extends org.apache.hadoop.mapreduce.Reducer<NullWritable, Text, NullWritable, Text> {

	private String delimiter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		delimiter = configuration.get("delimiter");
	}

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeMap<String, String> uniqueKeyMap = generateUniqueKeyMap(values);
		Collection<String> columns = uniqueKeyMap.values();

		int rank = 1;

		assert columns.iterator().hasNext();
		String previousPrimaryCoulmn = columns.iterator().next();

		Iterator<String> iterator = columns.iterator();
		while (iterator.hasNext()) {
			String row = iterator.next();
			String primaryColumn = getPrimaryCoulumn(row);

			StringBuilder builder = new StringBuilder(row);
			if(previousPrimaryCoulmn.equals(primaryColumn)){
				rank++;
			}else{
				previousPrimaryCoulmn = primaryColumn;
				rank = 1;
			}
			builder.append(delimiter).append(rank);
			context.write(NullWritable.get(), new Text(builder.toString()));
		}
	}

	private String getPrimaryCoulumn(String row) {
		int firstCommaIndex = row.indexOf(delimiter);
		return row.substring(0, firstCommaIndex);
	}

	private TreeMap<String, String> generateUniqueKeyMap(Iterable<Text> values) {
		TreeMap<String,String> uniqueKeyMap = new TreeMap<String,String>();
		Iterator<Text> interator = values.iterator();
		while (interator.hasNext()) {
			String row =  interator.next().toString();
			int lastCommaIndex = row.lastIndexOf(delimiter);
			String coulmns = row.substring(0, lastCommaIndex);
			String generateKey = row.substring(lastCommaIndex, row.length());
			uniqueKeyMap.put(generateKey, coulmns);
		}
		return uniqueKeyMap;
	}
}