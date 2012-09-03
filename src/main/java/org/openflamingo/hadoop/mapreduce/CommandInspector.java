package org.openflamingo.hadoop.mapreduce;

import org.apache.commons.cli.*;

/**
 * 이 클래스는 파라미터로 입력받은 args배열을 {@link CommandLine}로 만든다.
 *
 * @author Youngdeok Kim
 * @since JDK1.6
 */
public class CommandInspector {
	/**
	 * 필수 옵션
	 */
	private final String[][] requiredOptions =
		{
			{"input", "입력 경로를 지정해 주십시오. 입력 경로가 존재하지 않으면 MapReduce가 동작할 수 없습니다."},
			{"output", "출력 경로를 지정해 주십시오."},
			{"delimiter", "컬럼의 구분자를 지정해주십시오. CSV 파일의 컬럼을 처리할 수 없습니다."},
			{"command","명령을 입력 해주세요"}
		};

	/**
	 * 사용가능한 옵션 목록을 구성한다.
	 *
	 * @return 옵션 목록
	 */
	private Options getOptions() {
		Options options = new Options();
		options.addOption("input", true, "입력 경로 (필수)");
		options.addOption("output", true, "출력 경로 (필수)");
		options.addOption("delimiter", true, "컬럼 구분자 (필수)");
		options.addOption("command", true, "-command name");
		options.addOption("level", true, "-level 입력값");
		return options;
	}

	/**
	 * 터미널 명령어인 args배열을 분석하여 {@link CommandLine}객체를 만든다.
	 * @param args 터미널 args배열
	 * @return 명령어 분석이 끝난 {@link CommandLine} 객체
	 * @throws Exception
	 */
	public CommandLine parseCommandLine(String[] args) throws Exception {
		////////////////////////////////////////
		// 옵션 목록을 구성하고 검증한다.
		////////////////////////////////////////
		Options options = getOptions();
		HelpFormatter formatter = new HelpFormatter();
		if (args.length == 0) {
			formatter.printHelp("hadoop jar <JAR> " + getClass().getName(), options, true);
			throw new Exception();
		}

		// 커맨드 라인을 파싱한다.
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);

		// 파라미터를 검증한다.
		for (String[] requiredOption : requiredOptions) {
			if (!cmd.hasOption(requiredOption[0])) {
				formatter.printHelp("hadoop jar <JAR> " + getClass().getName(), options, true);
				throw new Exception();
			}
		}
		return cmd;
	}
}
