package org.openflamingo.hadoop.commons.job.createria;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public abstract class CriteriaJob<T> implements Criteria {
    protected T parameter;
    public CriteriaJob(T parameter) {
        this.parameter = parameter;
    }
}

