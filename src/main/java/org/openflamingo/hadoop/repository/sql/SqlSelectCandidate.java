package org.openflamingo.hadoop.repository.sql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;
import org.openflamingo.hadoop.repository.model.AprioriModel;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SqlSelectCandidate extends JdbcTemplate {
	private static final Log LOG = LogFactory.getLog(SqlSelectCandidate.class);
	public SqlSelectCandidate(MySQLConnector mySQLConnector) {
		super(mySQLConnector);
	}

	@Override
	protected String setSQL() {
		return SQL.SElECT_TBL_CANDIDATE;
	}

	@Override
	protected void setParameter(PreparedStatement pstmt, Object param) throws SQLException {
		AprioriModel model = (AprioriModel) param;
		pstmt.setString(1, model.getKey());
		pstmt.setString(2, model.getValue());
	}

	@Override
	protected Object resultMapping(ResultSet rs) throws SQLException {
		if(rs.first())
			return rs.getLong(1);
		return 0;
	}
}
