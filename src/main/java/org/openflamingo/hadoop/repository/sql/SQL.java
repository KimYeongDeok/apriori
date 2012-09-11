package org.openflamingo.hadoop.repository.sql;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class SQL {
    public static final String  CREATE_TBL_CADIDATE = "CREATE  TABLE IF NOT EXISTS yd.TBL_CANDIDATE (" +
                        "key_cadidate varchar(200) NOT NULL ," +
                        "value_cadidate VARCHAR(21844) NOT NULL ," +
                        "support BIGINT NOT NULL ,"+
                        "INDEX `IDX` USING BTREE (`key_cadidate` ASC));";

    public static final String CREATE_TBL_TOTALSIZE = "CREATE TABLE IF NOT EXISTS yd.TBL_TOTAL_SIZE (" +
                        "total_size BIGINT NOT NULL DEFAULT 0);";

    public static final String CREATE_TBL_SUPPORT = "CREATE TABLE IF NOT EXISTS yd.TBL_SUPPORT(" +
                        "key_cadidate VARCHAR(255) NOT NULL ," +
                        "support BIGINT NOT NULL, " +
                        "INDEX `IDX` USING BTREE (`key_cadidate` ASC));";
    public static final String INSERT_TBL_CADIDATE = "INSERT INTO yd.TBL_CANDIDATE (key_cadidate, value_cadidate, support)" +
            "VALUES (?, ?, ?);";

    public static final String INSERT_TBL_SUPPORT = "INSERT INTO yd.TBL_SUPPORT" +
            "(key_cadidate, support)" +
            "VALUES (?, ?);";

    public static final String UPDATE_TBL_TOTALSIZE = "UPDATE yd.TBL_TOTAL_SIZE SET total_size = ? limit 1;";
    public static final String SELECT_TBL_TOTALSIZE = "SELECT * FROM yd.TBL_TOTAL_SIZE;";
    public static final String INSERT_TBL_TOTALSIZE = "INSERT INTO  yd.TBL_TOTAL_SIZE (total_size) VALUES (0)";
}
