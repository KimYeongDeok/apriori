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
        PreparedStatement select = null;
        PreparedStatement udpate = null;
        ResultSet rs = null;
        PreparedStatement insert = null;
        try {
            conn.setTransactionIsolation(conn.TRANSACTION_SERIALIZABLE);

            select = conn.prepareStatement(SQL.SELECT_TBL_TOTALSIZE);
            rs = select.executeQuery();

            int size = 0;


            if (rs.first()) {
                size = rs.getInt(1);
            } else {
                insert = conn.prepareStatement(SQL.INSERT_TBL_TOTALSIZE);
                insert.executeUpdate();
            }

            int totalSize = size + (Integer) param;

            udpate = conn.prepareStatement(SQL.UPDATE_TBL_TOTALSIZE);
            udpate.setInt(1, totalSize);
            udpate.executeUpdate();
        } finally {
            try {
                if (select != null) {
                    select.close();
                }
            } catch (Exception e2) {
            }
            try {
                if (insert != null) {
                    insert.close();
                }
            } catch (Exception e2) {
            }
            try {
                if (udpate != null) {
                    udpate.close();
                }
            } catch (Exception e2) {
            }
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
            }
        }
        return null;
    }
}
