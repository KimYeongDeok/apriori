package org.openflamingo.hadoop.repository.sql;

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
public class SqlUpdateConfidenceAndLift extends JdbcTemplate {
    public SqlUpdateConfidenceAndLift(MySQLConnector mySQLConnector) {
        super(mySQLConnector);
    }

    @Override
    protected String setSQL() {
        return SQL.UPDATE_TBL_CANDIDATE_CONFIDENC;
    }

    @Override
    protected void setParameter(PreparedStatement pstmt, Object param) throws SQLException {
        AprioriModel model = (AprioriModel) param;
	    pstmt.setFloat(1, model.getConfidence());
	    pstmt.setFloat(2, model.getLift());
        pstmt.setString(3, model.getKey());
        pstmt.setString(4, model.getValue());

    }
}

