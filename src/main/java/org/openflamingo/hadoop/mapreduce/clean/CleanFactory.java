package org.openflamingo.hadoop.mapreduce.clean;

import org.openflamingo.hadoop.etl.utils.ArrayUtils;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CleanFactory {
	public static Clean create(String command, String delimeter) {
		int[] commands = ArrayUtils.stringToIntArray(command, delimeter);
		return new Clean(commands, delimeter);
	}
}
