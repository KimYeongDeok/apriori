package org.openflamingo.hadoop.repository.connector;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class MySQLConnectionPool {
    private static final String db = "yd";

    public MySQLConnectionPool() {
        PoolableObjectFactory factory = new BasePoolableObjectFactory() {
            @Override
            public Object makeObject() throws Exception {
                return new MySQLConnector("", db);
            }
        };
        ObjectPool pool = new GenericObjectPool(factory);
    }
}
