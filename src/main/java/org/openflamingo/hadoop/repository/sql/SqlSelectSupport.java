package org.openflamingo.hadoop.repository.sql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.builtin.LOG;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SqlSelectSupport extends JdbcTemplate {
	private static final Log LOG = LogFactory.getLog(SqlSelectSupport.class);
	public SqlSelectSupport(MySQLConnector mySQLConnector) {
		super(mySQLConnector);
	}

	@Override
	protected String setSQL() {
		return SQL.SELECT_TBL_SUPPORT;
	}

	@Override
	protected void setParameter(PreparedStatement pstmt, Object param) throws SQLException {
		pstmt.setString(1, (String) param);
	}

	@Override
	protected Object resultMapping(ResultSet rs) throws SQLException {
		if(rs.first())
			return rs.getLong(1);
		return 0;
	}
}
