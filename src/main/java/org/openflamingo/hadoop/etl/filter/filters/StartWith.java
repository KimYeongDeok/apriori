package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;

/**
 * 이 클래스는 컬럼의 정보중 처음 부분과 같은 정보인지 검사한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class StartWith extends FilterClass {
	@Override
	public boolean doFilter(String coulmn, FilterModel filterModel) {
		String target = filterModel.getTerms();
		int index = coulmn.lastIndexOf(target);
		return index == 0;

	}
}
