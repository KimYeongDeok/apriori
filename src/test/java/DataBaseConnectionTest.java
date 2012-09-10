import org.junit.Test;
import org.openflamingo.hadoop.repository.sql.JdbcTemplate;
import org.openflamingo.hadoop.repository.connector.MySQLConnector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class DataBaseConnectionTest {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(new MySQLConnector("127.0.0.1","yd")) {
        @Override
        protected String setSQL() {
            return "";
        }

        @Override
        protected void setParameter(PreparedStatement pstmt, Object param) throws SQLException {
        }
    };


	@Test
	public void test() throws Exception {
        new MySQLConnector("127.0.0.1","yd").createNotExsistTable();
//        jdbcTemplate.executeQuery();
	}
}
