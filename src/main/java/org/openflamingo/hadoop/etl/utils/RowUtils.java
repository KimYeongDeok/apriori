package org.openflamingo.hadoop.etl.utils;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class RowUtils {
	public static final String COMMAND_DELIMETER = "::";

	public static String[] parseByDelimeter(String row, String delimeter){
		String[] columns = row.split(delimeter);
		return columns;
	}

	public static String arrayToString(String[] coulmns, String delimeter){
		StringBuilder stringBuilder = new StringBuilder();
		for (String coulmn : coulmns) {
			stringBuilder.append(coulmn).append(delimeter);
		}
		stringBuilder.delete(stringBuilder.length()-1, stringBuilder.length());
		return stringBuilder.toString();
	}
}
