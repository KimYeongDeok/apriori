package org.openflamingo.hadoop.repository;

import com.google.common.hash.Hashing;
import org.openflamingo.hadoop.repository.connector.ConsistentHash;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;
import org.openflamingo.hadoop.repository.model.AprioriModel;
import org.openflamingo.hadoop.repository.sql.SqlInsertCadidate;
import org.openflamingo.hadoop.repository.sql.SqlInsertSupport;
import org.openflamingo.hadoop.repository.sql.SqlInsertTotalSize;

import java.util.ArrayList;
import java.util.List;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriRepositoryMySQL implements AprioriRepository{
    private ConsistentHash<MySQLConnector> consistentHash;
    private final String db = "yd";

    public AprioriRepositoryMySQL() {
        List<MySQLConnector> list = new ArrayList<MySQLConnector>();
        list.add(new MySQLConnector("192.168.0.3",db));
        list.add(new MySQLConnector("192.168.0.2",db));



        consistentHash = new ConsistentHash<MySQLConnector>(Hashing.md5(), 3, list);
    }


    public void saveCadidate(final String key, final String value, final int support) throws Exception {
        AprioriModel model = new AprioriModel();
        model.setKey(key);
        model.setValue(value);
        model.setSupport(support);

        MySQLConnector connector = consistentHash.get(key);

        SqlInsertCadidate insert = new SqlInsertCadidate(connector);

        insert.executeUpdate(model);
    }

    public void saveSupport(String key, int support) throws Exception {
        AprioriModel model = new AprioriModel();
        model.setKey(key);
        model.setSupport(support);

        MySQLConnector connector = consistentHash.get(key);
        SqlInsertSupport insert = new SqlInsertSupport(connector);

        insert.executeUpdate(model);
    }

    public void saveTotalSize(long size) throws Exception {
        MySQLConnector connector = consistentHash.get("0");

        SqlInsertTotalSize insert = new SqlInsertTotalSize(connector);
        insert.execute(size);
    }
}
