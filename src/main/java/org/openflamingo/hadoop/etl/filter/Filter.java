package org.openflamingo.hadoop.etl.filter;

/**
 * Filter의 기능을 수행 할 수있다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public interface Filter{
	/**
	 * Filter의 기능을 수행한다. Filter가 정상적으로 통과하면 true값을 반환하고
	 * 실패 할 경 우에는 false값을 반환한다.
	 * @param coulmns {@code Mapper}에서 입력받은 값을 분석한 데이터들의 배열
	 * @return 성공하면 true 실패하면 false
	 * @throws InterruptedException
	 */
	public boolean service(String[] coulmns) throws InterruptedException;

	/**
	 * {@code FilterModel}을 Filter에 설정 한다.
	 * @param filterModel Filter가 사용 할 FilterModel
	 */
	public void setFilterModel(FilterModel filterModel);
}
