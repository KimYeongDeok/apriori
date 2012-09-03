package org.openflamingo.hadoop.etl.filter;

/**
 * Filter의 기본적인 기능을 수행한다. Filter의 coulmn위치 값이 입력되지 않을 경우에는
 * 모든 column에 대하여 기능을 수행하고 coulmn위치 값이 있을 경우 한 coulmn만 기능을
 * 수행한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public abstract class FilterClass implements Filter {
	/**
	 * filterModel은 각 Filter에 전달될 정보
	 */
	protected FilterModel filterModel;

	public boolean service(String[] coulmns) throws InterruptedException {
		boolean success = false;

		if (coulmns.length <= filterModel.getColumnIndex())
			throw new InterruptedException("Out of CoulmnIndex" + coulmns.length);

		if (filterModel.getColumnIndex() < 0)
			success = doFilterInAllCoulmns(coulmns);
		else
			success = doFilterInCoulmn(coulmns, filterModel.getColumnIndex());

		return success;
	}

	/**
	 * 하나의 컬럼만 Filter를 수행한다. {@code columnIndex}의 컬럼 위치에서 Filter기능을
	 * 수행 한다. 정상적으로 Filter 기능을 통과 할 경우 true값을 반환하고 실패 할 경우 false
	 * 값을 반환한다.
	 *
	 * @param columns {@code Mapper}에서 입력받은 값을 분석한 데이터들의 배열
	 * @param columnIndex 컬럼의 위치 값
	 * @return 정상적으로 수행하면 true, 실패하면 false
	 */
	private boolean doFilterInCoulmn(String[] columns, int columnIndex) {
		String column = columns[columnIndex];
		return doFilter(column, filterModel);
	}

	/**
	 * 모든 컬럼에 대한 컬럼만 Filter를 수행한다. 모든 컬럼 위치에서 Filter기능을 수행한다.
	 * 정상적으로 Filter 기능을 통과 할 경우 true값을 반환하고 실패 할 경우 false 값을 반환한다.
	 *
	 * @param columns {@code Mapper}에서 입력받은 값을 분석한 데이터들의 배열
	 * @return 정상적으로 수행하면 true, 실패하면 false
	 */
	private boolean doFilterInAllCoulmns(String[] columns) {
		boolean isSuccess = true;
		for (String column : columns) {
			return doFilter(column, filterModel);
		}
		return false;
	}

	/**
	 * 각 기능의 Filter을 수행한다. 정상적으로 Filter 기능을 통과 할 경우
	 * true값을 반환하고 실패 할 경우 false 값을 반환한다.
	 *
	 * @param coulmn {@code Mapper}에서 입력받은 값을 분석하여 나온 컬럼
	 * @param filterModel Filter가 수행하기 위한 기본적인 정보
	 * @return 정상적으로 수행하면 true, 실패하면 false
	 */
	public abstract boolean doFilter(String coulmn, FilterModel filterModel);


	@Override
	public void setFilterModel(FilterModel filterModel) {
		this.filterModel = filterModel;
	}
}
