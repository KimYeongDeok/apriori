package org.openflamingo.hadoop.repository.sql;

import org.openflamingo.hadoop.repository.connector.MySQLConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SqlInsertTotalSize extends JdbcTemplate {
    public SqlInsertTotalSize(MySQLConnector mySQLConnector) {
        super(mySQLConnector);
    }

    @Override
    protected String setSQL() {
        return null;
    }

    @Override
    protected void setParameter(PreparedStatement pstmt, Object param) throws SQLException {
    }

    @Override
    protected Object executeService(Connection conn, Object param) throws SQLException {
        conn.setTransactionIsolation(conn.TRANSACTION_SERIALIZABLE);
        PreparedStatement select = conn.prepareStatement(SQL.SELECT_TBL_TOTALSIZE);
        ResultSet rs = select.executeQuery();

        int size=0;

        if(!rs.wasNull()){
            rs.first();
            size = rs.getInt(1);
        }
        int totalSize = size + (Integer)param;

        PreparedStatement udpate = conn.prepareStatement(SQL.UPDATE_TBL_TOTALSIZE);
        udpate.setInt(1, totalSize);
        udpate.executeUpdate();

        return null;
    }
}
