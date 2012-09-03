package org.openflamingo.hadoop.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * 구현된 ETL Driver를 수행한다. ETL Driver는 이 interface를 구현하여 FrontDriver에서
 * 기본적으로 설정된 부분외 구현된 ETL맞게 기능을 수행한다.
 *
 * @author Youngdeok Kim
 * @since JDK1.6
 */
public interface ETLDriver {
	/**
	 * FrontDriver에서 기본적인 설정 후에 각 구현된 ETL 맞게 수행한다.
	 *
	 * @param job FrontDriver에서 기본적으로 구현된 기본 {@link Job}
	 * @param cmd 터미널 명령을 분석한 {@link CommandLine}
	 * @param conf FrontDriver에서 기본적으로 생성된 {@link Configuration}
	 * @return 자바가상머신 종료 값
	 * @throws Exception
	 */
	int service(Job job, CommandLine cmd, Configuration conf) throws Exception;
}
