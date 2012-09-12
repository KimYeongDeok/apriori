package org.openflamingo.hadoop.repository;

import com.google.common.hash.Hashing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openflamingo.hadoop.repository.connector.ConsistentHash;
import org.openflamingo.hadoop.repository.connector.MySQLConnectionPool;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;
import org.openflamingo.hadoop.repository.model.AprioriModel;
import org.openflamingo.hadoop.repository.sql.SqlInsertCadidate;
import org.openflamingo.hadoop.repository.sql.SqlInsertSupport;
import org.openflamingo.hadoop.repository.sql.SqlInsertTotalSize;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriRepositoryMySQL implements AprioriRepository {
    private static final Log LOG = LogFactory.getLog(AprioriRepositoryMySQL.class);
    private final ExecutorService exec = Executors.newFixedThreadPool(10);
    private final ConsistentHash<MySQLConnectionPool> consistentHash;
    private final String db = "yd";

    public AprioriRepositoryMySQL() {
        List<MySQLConnectionPool> list = new ArrayList<MySQLConnectionPool>();
//        list.add(new MySQLConnector("192.168.0.3",db));
//        list.add(new MySQLConnector("192.168.0.2",db));
        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop2", 5));
        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop3", 5));
        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop4", 5));
        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop5", 5));
        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop6", 5));
        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop7", 5));
        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop8", 5));

        consistentHash = new ConsistentHash<MySQLConnectionPool>(Hashing.md5(), 3, list);
    }


    public void saveCadidate(final String key, final String value, final int support) throws Exception {
        exec.execute(new Runnable() {
            @Override
            public void run() {
                AprioriModel model = new AprioriModel();
                model.setKey(key);
                model.setValue(value);
                model.setSupport(support);

                MySQLConnectionPool connectionPool = consistentHash.get(key);
                MySQLConnector connector = null;
                try {
                    connector = connectionPool.getMySQLConnector();
                    SqlInsertCadidate insert = new SqlInsertCadidate(connector);
                    insert.executeUpdate(model);
                } catch (Exception e) {
                    LOG.error(e);
                } finally {
                    try {
                        connectionPool.returnMySQLConnector(connector);
                    } catch (Exception e) {
                        LOG.error(e);
                    }
                }
            }
        });
    }

    public void saveSupport(final String key, final int support) throws Exception {
        exec.execute(new Runnable() {
            @Override
            public void run() {
                AprioriModel model = new AprioriModel();
                model.setKey(key);
                model.setSupport(support);

                MySQLConnectionPool connectionPool = consistentHash.get(key);
                MySQLConnector connector = null;
                try {
                    SqlInsertSupport insert = new SqlInsertSupport(connector);
                    insert.executeUpdate(model);
                } catch (Exception e) {
                    LOG.error(e);
                } finally {
                    try {
                        connectionPool.returnMySQLConnector(connector);
                    } catch (Exception e) {
                        LOG.error(e);
                    }
                }
            }
        });
    }

    public void saveTotalSize(long size) throws Exception {
        MySQLConnectionPool connectionPool = consistentHash.get(0);
        MySQLConnector connector = connectionPool.getMySQLConnector();
        try {
            SqlInsertTotalSize insert = new SqlInsertTotalSize(connector);
            insert.execute(size);
        } finally {
            connectionPool.returnMySQLConnector(connector);
        }
    }
}
