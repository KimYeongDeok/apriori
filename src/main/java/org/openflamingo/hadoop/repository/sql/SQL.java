package org.openflamingo.hadoop.repository.sql;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SQL {
    public static final String CREATE_TBL_CANDIDATE = "CREATE  TABLE IF NOT EXISTS yd.TBL_CANDIDATE (" +
                        "key_candidate varchar(200) NOT NULL , " +
                        "value_candidate VARCHAR(10000) NOT NULL, " +
                        "support BIGINT NOT NULL DEFAULT 0, "+
					    "Confidence DECIMAL(10,3) NOT NULL DEFAULT 0, "+
					    "lift DECIMAL(10,3) NOT NULL DEFAULT 0, "+
                        "INDEX `IDX` USING BTREE (`key_candidate` ASC));";
	public static final String SElECT_TBL_CANDIDATE = "SELECT support FROM yd.TBL_CANDIDATE " +
		"WHERE key_candidate = ? " +
		"AND value_candidate = ?;";
	public static final String UPDATE_TBL_CANDIDATE_CONFIDENC = "UPDATE yd.TBL_CANDIDATE " +
		"SET confidence = ?, lift = ? " +
		"WHERE key_candidate = ? " +
		"AND value_candidate = ?;";


    public static final String CREATE_TBL_TOTALSIZE = "CREATE TABLE IF NOT EXISTS yd.TBL_TOTAL_SIZE (" +
                        "total_size BIGINT NOT NULL DEFAULT 0);";

    public static final String CREATE_TBL_SUPPORT = "CREATE TABLE IF NOT EXISTS yd.TBL_SUPPORT(" +
                        "key_candidate VARCHAR(255) NOT NULL ," +
                        "support BIGINT NOT NULL DEFAULT 0, " +
                        "INDEX `IDX` USING BTREE (`key_candidate` ASC));";
    public static final String INSERT_TBL_CANDIDATE = "INSERT INTO yd.TBL_CANDIDATE (key_candidate, value_candidate, support)" +
            "VALUES (?, ?, ?);";

    public static final String INSERT_TBL_SUPPORT = "INSERT INTO yd.TBL_SUPPORT" +
            "(key_candidate, support)" +
            "VALUES (?, ?);";
	public static final String SELECT_TBL_SUPPORT = "SELECT support FROM yd.TBL_SUPPORT where key_candidate = ?;";



	public static final String UPDATE_TBL_TOTALSIZE = "UPDATE yd.TBL_TOTAL_SIZE SET total_size = ? limit 1;";
    public static final String SELECT_TBL_TOTALSIZE = "SELECT * FROM yd.TBL_TOTAL_SIZE;";
    public static final String INSERT_TBL_TOTALSIZE = "INSERT INTO  yd.TBL_TOTAL_SIZE (total_size) VALUES (0)";


}
