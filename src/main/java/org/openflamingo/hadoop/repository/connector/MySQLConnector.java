package org.openflamingo.hadoop.repository.connector;

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
            Class.forName(MySQLConnector.JDBC_NAME);
            connection = DriverManager.getConnection(this.url, MySQLConnector.ID, MySQLConnector.PW);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createNotExsistTable() {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(SQL.CREATE_TBL_CADIDATE);
            pstmt.execute();

            pstmt = connection.prepareStatement(SQL.CREATE_TBL_TOTALSIZE);
            pstmt.execute();

            pstmt = connection.prepareStatement(SQL.CREATE_TBL_SUPPORT);
            pstmt.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropTable(){
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
            e.printStackTrace();
        }
    }


    public Connection getConnection() {
        return connection;
    }
}