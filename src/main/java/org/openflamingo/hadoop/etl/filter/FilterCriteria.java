package org.openflamingo.hadoop.etl.filter;

import org.openflamingo.hadoop.etl.ETLObjectHouse;
import org.openflamingo.hadoop.etl.utils.RowUtils;
import org.openflamingo.hadoop.util.StringUtils;

import java.util.ArrayList;

/**
 * Filter객체를 생성하여 처리한다. filter 명령어를 delimeter로 파싱하여
 * Filter객체를 생성한다. 또한 Filter객체 수행 역활을 돕는다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FilterCriteria {
	/** delimeter는 filter 명령어 파싱 할 구분자 */
	private String delimeter = ",";
	/** columns는 Filter를 수행 할 값 */
	private String[] columns;
	/** filters는 생성된 Filter객체를 보관*/
	private ArrayList<Filter> filters;


	public FilterCriteria(String filterCommand, String delimeter) {
		parseFilterCommand(filterCommand, delimeter);
	}

	/**
	 * Filter 명령어를 분석한다. Filter 명령어를 delimeter를 통해서 파싱하여
	 * Filter 객체를 추가한다.
	 * @param filterCommand Filter의 명령어
	 * @param delimeter Filter명령어 구분자
	 */
	private void parseFilterCommand(String filterCommand, String delimeter){
		String[] filterCommands = StringUtils.delimitedListToStringArray(filterCommand, delimeter);
		for (String filter : filterCommands) {
			String[] commands = RowUtils.parseByDelimeter(filter, RowUtils.COMMAND_DELIMETER);

			String commandName = commands[0];
			FilterModel filterModel = createFilterModel(commands);

			FilterClass filterClass = (FilterClass) ETLObjectHouse.findClassByName(commandName);
			filterClass.setFilterModel(filterModel);
			addFilter(filterClass);
		}
	}

	/**
	 * {@link FilterModel}를 생성한다. 입력받는 {@code commands}를 데이터 추출하여
	 * Filter객체를 만든다.
	 * @param commands Filter 명령어들
	 * @return {@link FilterModel} 생성된 {@link FilterModel}
	 */
	private FilterModel createFilterModel(String[] commands){
		int columnIndex = Integer.valueOf(commands[1]);
		String terms = "";
		if(commands.length > 2)
			terms = commands[2];

		FilterModel filterModel = new FilterModel();
		filterModel.setColumnIndex(columnIndex);
		filterModel.setTerms(terms);
		return filterModel;
	}

	/**
	 * Filter를 수행한다. {@code filters}에 저장된 Filter들을 순차적으로 진행하여
	 * 검사한다. 만약 모든 Filter를 통과하면 {@link FilterCriteria}를 반환하고 Filter를
	 * 통과하지 못하면 null를 반환한다.
	 * @param columns {@code Mapper}에서 입력받은 값을 분석한 데이터들의 배열
	 * @return {@link FilterCriteria}
	 * @throws InterruptedException
	 */
	public FilterCriteria doFilter(String[] columns) throws InterruptedException {
		this.columns = columns;

		if(filters == null || filters.size() == 0)
			return this;

		for (Filter filter : filters) {
			if(filter.service(this.columns))
				return passFilter();
		}
		this.columns = null;
		return this;
	}

	/**
	 * 모든 Filter가 통과하여 {@link FilterCriteria}를 반환한다.
	 * @return FilterCriteria
	 */
	private FilterCriteria passFilter() {
		return this;
	}

	/**
	 * Filter객체를 추가한다. {@code filters}에 입력받은 Filter를
	 * 추가한다.
	 * @param filter 추가 할 Filter
	 */
	public void addFilter(Filter filter){
		if(filters == null)
			filters = new ArrayList<Filter>();
		filters.add(filter);
	}

	/**
	 * String값으로 변환한 row 값을 반환하다. 모든 처리가 끝난 columns를
	 * String 으로 변환하여 반환한다.
	 * @return String
	 */
	public String getRow(){
		if(columns == null || columns.length == 0)
			return null;
		StringBuilder stringBuilder = new StringBuilder();
		for (String coulmn : columns) {
			stringBuilder.append(coulmn).append(delimeter);
		}
		stringBuilder.delete(stringBuilder.length()-1, stringBuilder.length());
		return stringBuilder.toString();
	}

	private String[] devideByDelimeter(String row) throws InterruptedException {
		if(row == null)
			throw new InterruptedException("row is null");
		if(row.length() == 0)
			throw new InterruptedException("row contains no data");
		return row.split(delimeter);
	}

	public boolean isRow() {
		return columns != null;
	}
}
