package org.openflamingo.hadoop.repository;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public interface AprioriRepository {
    public void saveCadidate(final String key, final String value) throws Exception;
    public void saveSupport(String key, int support) throws Exception;
    public void saveTotalSize(int size) throws Exception;
}
