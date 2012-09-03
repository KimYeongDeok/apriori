package org.openflamingo.hadoop.etl.replace;

/**
 * 이 클래스는 Replace 기능과 정보를 가지고 있다. {@link ReplaceCriteria}에서 생성된
 * Replace 정보로 하나의 Replace작업을 표시 할 수 있으며 기능을 수행 할 수 있다.
 *
 * @author Youngdeok Kim
 * @see ReplaceCriteria
 * @since 1.0
 */
public class Replace {
	/** coulmnIndex는 Replace 대상의 컬럼 위치 값  */
	private int coulmnIndex;
	/** target는 대상 컬럼 값 */
	private String target;
	/** replace는 대상 컬럼에 비교 대상 값 */
	private String replace;

	public Replace(int coulmnIndex, String target, String replace) {
		this.coulmnIndex = coulmnIndex;
		this.target = target;
		this.replace = replace;
	}

	public Replace(String[] command) throws InterruptedException {
		if(command.length != 3)
			throw new InterruptedException("Out of command lenghth");
		this.coulmnIndex = Integer.valueOf(command[0]);
		this.target = command[1];
		this.replace = command[2];
	}

	/**
	 * 컬럼 정보를 비교하여 교체한다. 설정된 컬럼위치에 정보를 비교하여 교체 할 정보와 다를 경우
	 * 설정된 {@code replace}로 교체 한다.
	 * @param columns {@code Mapper}로 부터 입력받은 row 데이터를 분석한 column정보에 대한 배열
	 */
	public void doReplace(String[] columns) {
		String column = columns[coulmnIndex];
		if(target.equals(column))
			columns[coulmnIndex] = replace;
	}
}
