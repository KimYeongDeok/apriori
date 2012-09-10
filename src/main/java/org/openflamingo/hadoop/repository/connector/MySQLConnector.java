package org.openflamingo.hadoop.repository.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openflamingo.hadoop.repository.sql.SQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class MySQLConnector {
    private static final Log LOG = LogFactory.getLog(MySQLConnector.class);
    public static final String JDBC_NAME = "com.mysql.jdbc.Driver";
    public static final String ID = "root";
    public static final String PW = "";
    private String url;
    private Connection connection;

    public MySQLConnector(String url, String db) {
        connectionDataBase(url, db);
    }

    private void connectionDataBase(String url, String db) {
        this.url = "jdbc:mysql://" + url + ":3306/" + db + "?autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=UTF-8﻿";
        try {
            Class.forName(JDBC_NAME);
            connection = DriverManager.getConnection(this.url, ID, PW);
        } catch (ClassNotFoundException e) {
            LOG.error(e);
        } catch (SQLException e) {
            LOG.error(e);
        }
    }

    public void createNotExsistTable() {
        PreparedStatement pstmt = null;

        try {
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(SQL.CREATE_TBL_CADIDATE);
            pstmt.execute();

            pstmt = connection.prepareStatement(SQL.CREATE_TBL_TOTALSIZE);
            pstmt.execute();

            pstmt = connection.prepareStatement(SQL.CREATE_TBL_SUPPORT);
            pstmt.execute();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            LOG.error(e);
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e2) {
            }
        }
    }

    public void dropTable() {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE yd.TBL_CANDIDATE");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE yd.TBL_TOTAL_SIZE");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE yd.TBL_SUPPORT");
            pstmt.execute();
            connection.commit();
        } catch (SQLException e) {
            LOG.error(e);
        }
    }


    public Connection getConnection() {
        return connection;
    }
}
