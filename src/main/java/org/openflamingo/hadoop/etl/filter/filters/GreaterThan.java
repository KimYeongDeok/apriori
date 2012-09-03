package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;

/**
 * 이 클래스는 숫자형태의 데이터로 컬럼의 정보가 보다 클 경우인지 검사한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GreaterThan extends FilterClass{
	@Override
	public boolean doFilter(String coulmn, FilterModel filterModel) {
		int thanValue = Integer.valueOf(filterModel.getTerms());
		int value = Integer.valueOf(coulmn);
		return value > thanValue;
	}
}
