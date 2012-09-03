package org.openflamingo.hadoop.mapreduce.group;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GroupCombiner  extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<Text> uniques = new HashSet<Text>();
		for (Text value : values) {
			if (uniques.add(value)) {
				context.write(key, value);
			}
		}
	}
}
