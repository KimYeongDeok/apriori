package org.openflamingo.hadoop.commons.utils;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ArrayUtils {
	public static int[] toIntArray(String[] strings) {
		int[] ints = new int[strings.length];
		for (int i = 0; i < ints.length; i++) {
			ints[i] = Integer.parseInt(strings[i]);
		}
		return ints;
	}
	public static int[] stringToIntArray(String string, String delimeter) {
		String[] strings = string.split(delimeter);
		return toIntArray(strings);
	}
}
