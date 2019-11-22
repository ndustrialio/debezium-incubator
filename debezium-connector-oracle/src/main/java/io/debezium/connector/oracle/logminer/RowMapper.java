/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * A utility class to map LogMiner content resultSet values.
 * This class gracefully logs errors, loosing an entry is not critical.
 * The loss will be logged
 */
public class RowMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowMapper.class);

    //operations
    public static final int INSERT = 1;
    public static final int DELETE = 2;
    public static final int UPDATE = 3;
    public static final int DDL = 5;
    public static final int COMMIT = 7;
    public static final int ROLLBACK = 36;

    private static final int SCN = 1;
    private static final int SQL_REDO = 2;
    private static final int OPERATION_CODE = 3;
    private static final int CHANGE_TIME = 4;
    private static final int TX_ID = 5;
    private static final int CSF = 6;
    private static final int TABLE_NAME = 7;
    private static final int SEG_OWNER = 8;

    public static int getOperationCode(ResultSet rs) {
        try {
            return rs.getInt(OPERATION_CODE);
        } catch (SQLException e) {
            logError(e, "OPERATION_CODE");
            return 0;
        }
    }

    public static String getTableName(ResultSet rs)  {
        try {
            return rs.getString(TABLE_NAME);
        } catch (SQLException e) {
            logError(e, "TABLE_NAME");
            return "";
        }
    }

    public static String getSegOwner(ResultSet rs)  {
        try {
            return rs.getString(SEG_OWNER);
        } catch (SQLException e) {
            logError(e, "SEG_OWNER");
            return "";
        }
    }

    public static Timestamp getChangeTime(ResultSet rs) {
        try {
            return rs.getTimestamp(CHANGE_TIME);
        } catch (SQLException e) {
            logError(e, "CHANGE_TIME");
            return new Timestamp(Instant.now().getEpochSecond());
        }
    }

    public static BigDecimal getScn(ResultSet rs) {
        try {
            return rs.getBigDecimal(SCN);
        } catch (SQLException e) {
            logError(e, "SCN");
            return new BigDecimal(-1);
        }
    }

    public static String getTransactionId(ResultSet rs) {
        try {
            return DatatypeConverter.printHexBinary(rs.getBytes(TX_ID));
        } catch (SQLException e) {
            logError(e, "TX_ID");
            return "";
        }
    }

    public static String getSqlRedo(ResultSet rs) {
        StringBuilder result = new StringBuilder();
        try {
            int csf = rs.getInt(CSF);
            // 0 - indicates SQL_REDO is contained within the same row
            // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
            // the next row returned by the ResultSet
            if (csf == 0) {
                return rs.getString(SQL_REDO);
            } else {
                result = new StringBuilder(rs.getString(SQL_REDO));
                int lobLimit = 10000; // todo : decide on approach ( XStream chunk option) and Lob limit
                BigDecimal scn = getScn(rs);
                while (csf == 1) {
                    rs.next();
                    if (lobLimit-- == 0) {
                        LOGGER.warn("LOB value for SCN= {} was truncated due to the connector limitation of {} MB", scn, 40);
                        break;
                    }
                    csf = rs.getInt(CSF);
                    result.append(rs.getString(SQL_REDO));
                }
            }
        } catch (SQLException e) {
            logError(e, "SQL_REDO");
        }
        return result.toString();
    }

    static void logError(SQLException e, String s) {
        LOGGER.error("Cannot get {}. This entry from log miner will be lost due to the {}", s, e);
    }

    public static TableId getTableId(String catalogName, ResultSet rs) throws SQLException {
        return new TableId(catalogName.toUpperCase(), rs.getString(SEG_OWNER).toUpperCase(), rs.getString(TABLE_NAME).toUpperCase());
    }

}
