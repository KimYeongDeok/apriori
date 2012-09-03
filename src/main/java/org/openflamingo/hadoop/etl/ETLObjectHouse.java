package org.openflamingo.hadoop.etl;

import org.openflamingo.hadoop.etl.filter.filters.*;
import org.openflamingo.hadoop.mapreduce.aggregate.AggregateDriver;
import org.openflamingo.hadoop.mapreduce.apriori.AprioriDriver;
import org.openflamingo.hadoop.mapreduce.clean.CleanDriver;
import org.openflamingo.hadoop.mapreduce.filter.FilterDriver;
import org.openflamingo.hadoop.mapreduce.generate.GenerateDriver;
import org.openflamingo.hadoop.mapreduce.grep.GrepDriver;
import org.openflamingo.hadoop.mapreduce.group.GroupDriver;
import org.openflamingo.hadoop.mapreduce.rank.RankDriver;
import org.openflamingo.hadoop.mapreduce.replace.ReplaceDriver;

import java.util.HashMap;
import java.util.Map;

/**
 * ETL에서 사용되는 모든 객체를 보관하고 있다. ETL에서 사용되는 객체들을
 * 생성하고 보관하여 어디서든지 사용 할 수 있게 한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ETLObjectHouse {
	/** objectMap은 ETL객체들을 보관 */
	private static Map<String, Object> objectMap = new HashMap<String, Object>();

	/**
	 * 객체 생성
	 */
	static {
		objectMap.put("aggregate", new AggregateDriver());
		objectMap.put("clean", new CleanDriver());
		objectMap.put("filter", new FilterDriver());
		objectMap.put("grep", new GrepDriver());
		objectMap.put("replace", new ReplaceDriver());
		objectMap.put("generate", new GenerateDriver());
		objectMap.put("rank", new RankDriver());
		objectMap.put("group", new GroupDriver());
        objectMap.put("apriori", new AprioriDriver());

		objectMap.put("EQ", new Equals());
		objectMap.put("NEQ", new NotEquals());
		objectMap.put("EMPTY", new Empty());
		objectMap.put("NEMPTY", new NotEmpty());
		objectMap.put("STARTWITH", new StartWith());
		objectMap.put("ENDWITH", new EndWith());
		objectMap.put("GT", new GreaterThan());
		objectMap.put("GTE", new GreaterThanEquals());
		objectMap.put("LT", new LeastThan());
		objectMap.put("LTE", new LeastThanEquals());
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
