package org.openflamingo.hadoop.etl.replace;

import org.openflamingo.hadoop.etl.utils.RowUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code Replace} 정보를 보관하며 기능 수행한다. 입력된 repace 명령어를 분석하여 {@code Replace}객체를
 * 생성하고 생성된 {@code Replace} 기능 수행을 한다.
 *
 * @author Youngdeok Kim
 * @see Replace
 * @since 1.0
 */
public class ReplaceCriteria {
	/** 생성된 Replace객체를 보관 */
	private List<Replace> replaces;

	/**
	 * 입력된 replace 명령어를 delimiter로 분석하여 {@link Replace}객체를 추가한다.
	 * @param replaceCommand Replace 명령어
	 * @param delimiter Replace 명령어를 분석 할때 쓰이는 구분자
	 * @throws InterruptedException
	 */
	public void parseReplaceCommand(String replaceCommand, String delimiter) throws InterruptedException {
		String[] columns = RowUtils.parseByDelimeter(replaceCommand, delimiter);
		for (String column : columns) {
			String[] command = RowUtils.parseByDelimeter(column, RowUtils.COMMAND_DELIMETER);
			addReplace(new Replace(command));
		}
	}

	/**
	 * {@link Replace} 객체를 추가한다.
	 * @param replace {@link Replace} 객체
	 */
	private void addReplace(Replace replace){
		if(replaces == null)
			replaces = new ArrayList<Replace>();
		replaces.add(replace);
	}

	/**
	 * 보관되어 있는 모든 Replace의 기능을 수행한다.
	 * @param columns {@code Mapper}로 부터 입력받은 row 데이터를 분석한 column정보에 대한 배열
	 */
	public void doReplace(String[] columns){
		for (Replace replace : replaces) {
			replace.doReplace(columns);
		}
	}
}
