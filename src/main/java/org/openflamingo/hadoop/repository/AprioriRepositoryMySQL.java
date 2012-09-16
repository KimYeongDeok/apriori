package org.openflamingo.hadoop.repository;

import com.google.common.hash.Hashing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openflamingo.hadoop.commons.MapReduceConfigration;
import org.openflamingo.hadoop.repository.connector.ConsistentHash;
import org.openflamingo.hadoop.repository.connector.MySQLConnectionPool;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;
import org.openflamingo.hadoop.repository.model.AprioriModel;
import org.openflamingo.hadoop.repository.sql.*;

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
	private final ExecutorService exec = Executors.newFixedThreadPool(MapReduceConfigration.NUMBER_THREADPOOL);
	private final ConsistentHash<MySQLConnectionPool> consistentHash;

	public AprioriRepositoryMySQL() {
		List<MySQLConnectionPool> list = new ArrayList<MySQLConnectionPool>();
		list.add(new MySQLConnectionPool(MySQLConnector.class, MapReduceConfigration.URL, MapReduceConfigration.NUMBER_CONNECTIONPOOL));
		list.add(new MySQLConnectionPool(MySQLConnector.class, MapReduceConfigration.URL, MapReduceConfigration.NUMBER_CONNECTIONPOOL));
		list.add(new MySQLConnectionPool(MySQLConnector.class, MapReduceConfigration.URL, MapReduceConfigration.NUMBER_CONNECTIONPOOL));
//        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop2", MapReduceConfigration.NUMBER_CONNECTIONPOOL));
//        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop3", MapReduceConfigration.NUMBER_CONNECTIONPOOL));
//        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop4", MapReduceConfigration.NUMBER_CONNECTIONPOOL));
//        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop5", MapReduceConfigration.NUMBER_CONNECTIONPOOL));
//        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop6", MapReduceConfigration.NUMBER_CONNECTIONPOOL));
//        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop7", MapReduceConfigration.NUMBER_CONNECTIONPOOL));
//        list.add(new MySQLConnectionPool(MySQLConnector.class, "hadoop8", MapReduceConfigration.NUMBER_CONNECTIONPOOL));

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
	public void saveConfidenceAndLift(final AprioriModel model){
		exec.execute(new Runnable() {
			@Override
			public void run() {
				MySQLConnectionPool connectionPool = consistentHash.get(model.getKey());
				MySQLConnector connector = null;
				try {
					connector = connectionPool.getMySQLConnector();
					SqlUpdateConfidenceAndLift update = new SqlUpdateConfidenceAndLift(connector);
					update.executeUpdate(model);
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
					connector = connectionPool.getMySQLConnector();
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

	public long findSupportByKey(final String key) throws Exception {
		long result = 0;
		MySQLConnectionPool connectionPool = consistentHash.get(key);
		MySQLConnector connector = null;

		try {
			connector = connectionPool.getMySQLConnector();
			SqlSelectSupport select = new SqlSelectSupport(connector);
			result = (Long) select.executeQuery(key);
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			try {
				connectionPool.returnMySQLConnector(connector);
			} catch (Exception e) {
				LOG.error(e);
			}
		}
		return result;
	}

	public long findCandidateByKey(final String key, final String value) throws Exception {
		long result = 0;
		AprioriModel model = new AprioriModel();
		model.setKey(key);
		model.setValue(value);

		MySQLConnectionPool connectionPool = consistentHash.get(key);
		MySQLConnector connector = null;

		try {
			connector = connectionPool.getMySQLConnector();
			SqlSelectCandidate select = new SqlSelectCandidate(connector);
			result = (Long) select.executeQuery(model);
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			try {
				connectionPool.returnMySQLConnector(connector);
			} catch (Exception e) {
				LOG.error(e);
			}
		}
		return result;
	}
}
