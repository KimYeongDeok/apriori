package org.openflamingo.hadoop.etl.filter;

/**
 * Filter를 수행하기 위한 정보를 담는다. Filter를 수행하기 위해서 필요한 정보
 * {@code columnIndex}, {@code terms}를 보관한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FilterModel {
	/** terms는 각 Filter에 수행하기 위한 대상 값 */
	private String terms;
	/** columnIndex는 각 Filter에 수행 할 column 값*/
	private int columnIndex = -1;

	/** getter & setter */
	public String getTerms() {
		return terms;
	}

	public void setTerms(String terms) {
		this.terms = terms;
	}

	public int getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FilterModel that = (FilterModel) o;

		if (columnIndex != that.columnIndex) return false;
		if (terms != null ? !terms.equals(that.terms) : that.terms != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = terms != null ? terms.hashCode() : 0;
		result = 31 * result + columnIndex;
		return result;
	}
}
