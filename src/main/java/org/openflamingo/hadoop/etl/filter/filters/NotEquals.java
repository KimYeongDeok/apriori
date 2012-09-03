package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;

/**
 * 이 클래스는 컬럼의 정보와 다른 정보인지 검사한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class NotEquals extends FilterClass{
	@Override
	public boolean doFilter(String coulmn, FilterModel filterModel) {
		return !filterModel.getTerms().equals(coulmn);
	}
}
