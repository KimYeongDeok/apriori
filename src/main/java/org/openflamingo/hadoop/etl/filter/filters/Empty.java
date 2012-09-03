package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;

/**
 * 이 클래스는 컬럼의 빈값 혹은 빈공간을 가지고 있는지 검사한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class Empty extends FilterClass{

	@Override
	public boolean doFilter(String coulmn, FilterModel filterModel) {
		String trimmedCoulmn = coulmn.trim();
		return "".equals(trimmedCoulmn);
	}
}
