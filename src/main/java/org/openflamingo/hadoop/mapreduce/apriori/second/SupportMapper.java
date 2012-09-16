package org.openflamingo.hadoop.mapreduce.apriori.second;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openflamingo.hadoop.repository.AprioriRepositoryMySQL;
import org.openflamingo.hadoop.repository.model.AprioriModel;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SupportMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(SupportMapper.class);
	private AprioriRepositoryMySQL repository;
	private String delimiter;
	private long totalSize;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		delimiter = configuration.get("delimiter");
		totalSize = configuration.getLong("totalSize", 0);
		if (totalSize == 0)
			LOG.error("totalSize : 0");
		repository = new AprioriRepositoryMySQL();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tempRow = value.toString();
		int indexSpace = tempRow.indexOf("\t");

		String firstRow = tempRow.substring(0, indexSpace);
		String secondRow = tempRow.substring(indexSpace + 1, tempRow.length());

		long supportKey = findSupport(firstRow);
		long supportValue = findSupport(secondRow);
		long supportKeywithValue = findCandidate(firstRow, secondRow);

		if (supportKey == 0 || supportValue == 0 || supportKeywithValue == 0)
			return;

		float supportA = (float) supportKey / totalSize;
		float supportB = (float) supportValue / totalSize;
		float supportAwithB = (float)supportKeywithValue / totalSize;

		float tempConfidence = (supportA/100);
		float confidence = (supportAwithB/100) / tempConfidence;

		float tempLift = (supportA/100) * (supportB/100);
		float lift = ((supportAwithB/100) / tempLift)/100;

		AprioriModel model = new AprioriModel();
		model.setKey(firstRow);
		model.setValue(secondRow);
		model.setConfidence(confidence);
		model.setLift(lift);

		repository.saveConfidenceAndLift(model);
	}

	private long findCandidate(String firstRow, String secondRow) {
		long supportAwithB = 0;
		try {
			return repository.findCandidateByKey(firstRow, secondRow);
		} catch (Exception e) {
			LOG.error(e);
		}
		return supportAwithB;
	}


	private long findSupport(String firstRow) {
		try {
			return repository.findSupportByKey(firstRow);
		} catch (Exception e) {
			LOG.error(e);
		}
		return 0;
	}
}
