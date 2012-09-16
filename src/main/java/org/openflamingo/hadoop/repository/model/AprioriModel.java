package org.openflamingo.hadoop.repository.model;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriModel {
    private String key;
    private String value;
    private long support;
	private float lift;
	private float confidence;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getSupport() {
        return support;
    }

    public void setSupport(long support) {
        this.support = support;
    }

	public float getLift() {
		return lift;
	}

	public void setLift(float lift) {
		this.lift = lift;
	}

	public float getConfidence() {
		return confidence;
	}

	public void setConfidence(float confidence) {
		this.confidence = confidence;
	}

	@Override
	public String toString() {
		return "AprioriModel{" +
			"key='" + key + '\'' +
			", value='" + value + '\'' +
			", support=" + support +
			", lift=" + lift +
			", confidence=" + confidence +
			'}';
	}
}
