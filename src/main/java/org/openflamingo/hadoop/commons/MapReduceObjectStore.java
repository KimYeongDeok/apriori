package org.openflamingo.hadoop.commons;

import org.openflamingo.hadoop.mapreduce.apriori.AprioriDriver;

import java.util.HashMap;
import java.util.Map;

/**
 * ETL에서 사용되는 모든 객체를 보관하고 있다. ETL에서 사용되는 객체들을
 * 생성하고 보관하여 어디서든지 사용 할 수 있게 한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class MapReduceObjectStore {
	/** objectMap은 ETL객체들을 보관 */
	private static Map<String, Object> objectMap = new HashMap<String, Object>();

	/**
	 * 객체 생성
	 */
	static {
        objectMap.put("apriori", new AprioriDriver());
	}


	/**
	 * ETL 객체 찾아낸다. ETL객체 이름으로 검색하여 반환한다. 하지만
	 * 없을 경우에는 null를 반환한다.
	 * @param name ETL 객체 이름
	 * @return ETL 객체
	 */
	public static Object findClassByName(String name) {
		if (objectMap.containsKey(name))
			return objectMap.get(name);
		return null;
	}
}
