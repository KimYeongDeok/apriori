package org.openflamingo.hadoop.mapreduce.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openflamingo.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kimou
 * @since 1.0
 */
public class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String delimiter;
	private int groupColumn;
	private int reduceCoulumn;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		delimiter = configuration.get("delimiter");
		String group = configuration.get("group");

		String[] commands = StringUtils.delimitedListToStringArray(group, delimiter);
		groupColumn = Integer.valueOf(commands[0]);
		reduceCoulumn = Integer.valueOf(commands[1]);
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columns = StringUtils.delimitedListToStringArray(value.toString(), delimiter);

		for (int index = 0; index < columns.length; index++) {
			if (index == groupColumn)
				continue;
			if(index == reduceCoulumn)
				context.write(new Text(columns[groupColumn]), new Text(columns[index]));
		}
	}
}

