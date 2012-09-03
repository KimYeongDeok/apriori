package org.openflamingo.hadoop.mapreduce.clean;

import org.openflamingo.hadoop.util.StringUtils;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class Clean {

	private String delimeter;

	private int[] columnIndexs;

	public Clean(int[] columnIndexs, String delimeter) {
		this.columnIndexs = columnIndexs;
		this.delimeter = delimeter;
	}

	public String doClean(String row) {
		String[] columns = StringUtils.delimitedListToStringArray(row, delimeter);

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < columns.length; i++) {
			if (checkCleanColumn(i))
				continue;
			builder.append(columns[i]).append(delimeter);
		}

		builder.delete(builder.length() - 1, builder.length());
		return builder.toString();
	}

	private boolean checkCleanColumn(int index) {
		boolean result = false;
		for (int columnIndex : columnIndexs) {
			if (index == columnIndex)
				return true;
		}
		return result;
	}
}
