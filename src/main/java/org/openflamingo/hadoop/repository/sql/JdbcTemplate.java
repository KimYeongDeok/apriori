package org.openflamingo.hadoop.repository.sql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;

import java.sql.*;

public abstract class JdbcTemplate {
	private static final Log LOG = LogFactory.getLog(JdbcTemplate.class);
    private final MySQLConnector mySQLConnector;

    public JdbcTemplate(MySQLConnector mySQLConnector) {
        this.mySQLConnector = mySQLConnector;
    }

    public Object executeQuery() throws Exception {
        Object returnObject = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Connection conn = mySQLConnector.getConnection();
        try {
            pstmt = conn.prepareStatement(setSQL());
            setParameter(pstmt, null);
            conn.setAutoCommit(false);
            rs = pstmt.executeQuery();
            conn.commit();
            conn.setAutoCommit(true);
            returnObject = resultMapping(rs);
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e2) {
                throw e2;
            }
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e2) {
                throw e2;
            }
        }
        return returnObject;
    }
	public Object executeQuery(Object param) throws Exception {
       Object returnObject = null;
       PreparedStatement pstmt = null;
       ResultSet rs = null;
       Connection conn = mySQLConnector.getConnection();
       try {
           pstmt = conn.prepareStatement(setSQL());
           setParameter(pstmt, param);
           conn.setAutoCommit(false);
           rs = pstmt.executeQuery();
           conn.commit();
           conn.setAutoCommit(true);
           returnObject = resultMapping(rs);
       } catch (Exception e) {
           conn.rollback();
           throw e;
       } finally {
           try {
               if (rs != null) {
                   rs.close();
               }
           } catch (Exception e2) {
               throw e2;
           }
           try {
               if (pstmt != null) {
                   pstmt.close();
               }
           } catch (Exception e2) {
               throw e2;
           }
       }
       return returnObject;
   }

    public Object executeUpdate() throws Exception {
        Object returnObject = null;
        PreparedStatement pstmt = null;
        Connection conn = mySQLConnector.getConnection();
        try {
            pstmt = conn.prepareStatement(setSQL());
            setParameter(pstmt, null);
            conn.setAutoCommit(false);
            pstmt.executeUpdate();
            conn.commit();
            conn.setAutoCommit(true);
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e2) {
                throw e2;
            }
        }
        return returnObject;
    }

    public Object executeUpdate(Object param) throws Exception {
           Object returnObject = null;
           PreparedStatement pstmt = null;
           Connection conn = mySQLConnector.getConnection();
           try {
               pstmt = conn.prepareStatement(setSQL());
               setParameter(pstmt, param);
               conn.setAutoCommit(false);
               pstmt.executeUpdate();
               conn.commit();
               conn.setAutoCommit(true);
           } catch (Exception e) {
               conn.rollback();
               throw e;
           } finally {
               try {
                   if (pstmt != null) {
                       pstmt.close();
                   }
               } catch (Exception e2) {
                   throw e2;
               }
           }
           return returnObject;
       }

    public Object execute(Object param) throws Exception {
        Object returnObject = null;
        PreparedStatement pstmt = null;
        Connection conn = mySQLConnector.getConnection();
        try {
            conn.setAutoCommit(false);
            returnObject = executeService(conn, param);
            conn.commit();
            conn.setAutoCommit(true);
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e2) {
                throw e2;
            }
        }
        return returnObject;
    }



    protected Object executeService(Connection conn, Object param) throws SQLException {
        return null;
    }

    protected abstract String setSQL();

    protected abstract void setParameter(PreparedStatement pstmt, Object param) throws SQLException;

    protected Object resultMapping(ResultSet rs) throws SQLException {
        return null;
    }
}