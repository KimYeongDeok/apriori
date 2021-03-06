package org.openflamingo.hadoop.repository.sql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;
import org.openflamingo.hadoop.repository.model.AprioriModel;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SqlInsertSupport extends JdbcTemplate{
    private static final Log LOG = LogFactory.getLog(SqlInsertSupport.class);
    public SqlInsertSupport(MySQLConnector mySQLConnector) {
        super(mySQLConnector);
    }

    @Override
    protected String setSQL() {
        return SQL.INSERT_TBL_SUPPORT;
    }

    @Override
    protected void setParameter(PreparedStatement pstmt, Object param) throws SQLException {
        AprioriModel model = (AprioriModel) param;
        pstmt.setString(1, model.getKey());
        pstmt.setLong(2, model.getSupport());
    }
}
