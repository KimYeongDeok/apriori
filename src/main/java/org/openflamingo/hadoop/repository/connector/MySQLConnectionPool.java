package org.openflamingo.hadoop.repository.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    private static final Log LOG = LogFactory.getLog(MySQLConnectionPool.class);
    private final ObjectPool pool;

    public MySQLConnectionPool(final Class clz, final String url, int repeat) {
        PoolableObjectFactory factory = new BasePoolableObjectFactory() {
            @Override
            public Object makeObject() throws Exception {
                MySQLConnector connector = (MySQLConnector) clz.newInstance();
                connector.connectionDataBase(url);
                return connector;
            }
        };
        pool = new GenericObjectPool(factory, repeat);

        try {
            for (int i = 0; i < repeat; i++) {
                pool.addObject();
            }
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    public MySQLConnector getMySQLConnector() throws Exception {
        return (MySQLConnector) pool.borrowObject();
    }

    public void returnMySQLConnector(MySQLConnector connector) throws Exception {
        if (connector == null)
            return;
        pool.returnObject(connector);
    }
}
