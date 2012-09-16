package org.openflamingo.hadoop.commons.sequence;

import java.util.ArrayList;
import java.util.List;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SequencFilePath {
	private List<String> filePaths;
	public String filePath;
	public int sequenc;

	public SequencFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String next(){
		return filePath + (++sequenc);
	}
	public String current(){
		return filePath + sequenc;
	}

	public String currentMapReduceFile(){
		return filePath + sequenc+"/part*";
	}

	public String getFilePath(){
		return filePath;
	}
}
