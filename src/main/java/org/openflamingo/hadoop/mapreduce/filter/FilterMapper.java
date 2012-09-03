package org.openflamingo.hadoop.mapreduce.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openflamingo.hadoop.etl.filter.FilterCriteria;
import org.openflamingo.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private FilterCriteria criteria;
	private String delimeter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		delimeter = configuration.get("delimiter");
		String filter = configuration.get("filter");

		criteria = new FilterCriteria(filter, delimeter);

		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columns = StringUtils.delimitedListToStringArray(value.toString(), ",");

		criteria.doFilter(columns);

		if(criteria.isRow())
			context.write(NullWritable.get(), new Text(criteria.getRow()));
	}
}
