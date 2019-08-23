/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * This utility class contains SQL statements to configure, manage and query Oracle LogMiner
 */
public class SqlUtils {

    static final String BUILD_DICTIONARY = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
    static final String ONLINE_LOG_FILENAME = "SELECT min(MEMBER), group# FROM V$LOGFILE group by group# order by 2";
    static final String CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";
    static final String START_LOGMINER_FOR_ARCHIVE_STATEMENT = "BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(" +
            "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;";
    static final String END_LOGMNR = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";
    static final String OLDEST_FIRST_CHANGE = "SELECT MIN(FIRST_CHANGE#) FROM V$LOG";
    static final String OLDEST_ARCHIVED_CHANGE = "SELECT MIN(FIRST_CHANGE#) FROM V$ARCHIVED_LOG";
    static final String LATEST_SCN_FROM_ARCHIVED_LOG = "SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG"; // todo replace with ARCHIVELOG_CHANGE# from v$database
    static final String ALL_ARCHIVED_LOGS_NAMES_FOR_OFFSET = "SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE " +
            "FROM V$ARCHIVED_LOG WHERE FIRST_CHANGE# BETWEEN ? AND ? AND STATUS = 'A' AND STANDBY_DEST='NO' ORDER BY NEXT_CHANGE ASC";
    static final String ALL_ONLINE_LOGS_NAMES_FOR_OFFSET = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE " +
            "            FROM V$LOG L, V$LOGFILE F " +
            "            WHERE F.GROUP# = L.GROUP# " +
            "            GROUP BY L.NEXT_CHANGE#" +
            "            ORDER BY 2";

    static final String REDO_LOGS_STATUS = "SELECT F.MEMBER, R.STATUS FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# ORDER BY 2";
    static final String REDO_LOG_FILES_STATUS = "SELECT MEMBER, STATUS FROM V$LOGFILE ORDER BY 2";
    static final String REDO_LOGS_SEQUENCE = "SELECT F.MEMBER, R.SEQUENCE# FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# order by 2";
    static final String SWITCH_HISTORY = "SELECT F.MEMBER, SUBSTR(TO_CHAR(H.FIRST_TIME, 'HH24:MI'), 1, 15) SWITCH_TIME" +
            "   FROM V$LOG_HISTORY H, V$LOG L, V$LOGFILE F " +
            "   WHERE H.SEQUENCE# = L.SEQUENCE#" +
            "   AND F.GROUP#=L.GROUP#" +
            "   AND L.FIRST_TIME > TRUNC(SYSDATE) ORDER BY H.SEQUENCE# ASC";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

    // todo handle INVALID file member (report somehow and continue to work with valid file), handle adding multiplexed files,
    // todo SELECT name, value FROM v$sysstat WHERE name = 'redo wastage';
    // todo SELECT GROUP#, STATUS, MEMBER FROM V$LOGFILE WHERE STATUS='INVALID'; (drop and recreate? or do it manually?)
    // todo SELECT BLOCKSIZE FROM V$LOG;

    static final String NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
            + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
            + "  NLS_NUMERIC_CHARACTERS = '.,'";

    /**
     * This returns statement to build log miner view for online redo log files
     * @param startScn mine from
     * @param endScn mine till
     * @param strategy Log Mining strategy
     * @return statement
     */
    static String getStartLogMinerStatement(Long startScn, Long endScn, OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining) {
        String miningStrategy;
        String ddlTracking = "";
        if (strategy.equals(OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO)) {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS";
            ddlTracking =  " DBMS_LOGMNR.DDL_DICT_TRACKING + ";
        } else {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG";
        }
        if (isContinuousMining) {
            miningStrategy += " + DBMS_LOGMNR.CONTINUOUS_MINE ";
            ddlTracking = "";
        }
        return "BEGIN sys.dbms_logmnr.start_logmnr(" +
                "startScn => '" + startScn + "', " +
                "endScn => '" + endScn + "', " +
                "OPTIONS => " + miningStrategy + " + " + ddlTracking +
                "DBMS_LOGMNR.NO_ROWID_IN_STMT);" +
                "END;";
    }

    /**
     * This is the query from the log miner view to get changes. Columns of the view we using are:
     *
     * SCN - The SCN at which a change was made
     * COMMIT_SCN - The SCN at which a change was committed
     * OPERATION - User level SQL operation that made the change.
     * USERNAME - Name of the user who executed the transaction
     * SQL_REDO Reconstructed SQL statement that is equivalent to the original SQL statement that made the change
     * SEG_TYPE - Segment type. Possible values are:
     * STATUS - A value of 0 indicates that the reconstructed SQL statements as shown
     *          in the SQL_REDO and SQL_UNDO columns are valid executable SQL statements
     * OPERATION_CODE - Number of the operation code.
     * TABLE_NAME - Name of the modified table
     * TIMESTAMP - Timestamp when the database change was made
     * COMMIT_TIMESTAMP - Timestamp when the transaction committed
     *
     * @param schemaName user name
     * @param logMinerUser log mining session user name
     * @param schema schema
     * @return the query
     */

    public static String queryLogMinerContents(String schemaName, String logMinerUser, OracleDatabaseSchema schema)  {
        List<String> whiteListTableNames = schema.tableIds().stream().map(TableId::table).collect(Collectors.toList());

        return "SELECT SCN, COMMIT_SCN, OPERATION, USERNAME, SRC_CON_NAME, SQL_REDO, SEG_TYPE, " +
                        " STATUS, OPERATION_CODE, TABLE_NAME, TIMESTAMP, COMMIT_TIMESTAMP, XID, CSF, " +
                " SEG_OWNER, SEG_NAME, SEQUENCE# " +
                        " FROM V$LOGMNR_CONTENTS " +
                        " WHERE " +
                        // currently we do not capture changes from other schemas
                        " USERNAME = '"+ schemaName.toUpperCase() +"'" +
                        " AND OPERATION_CODE in (1,2,3,5) " +// 5 - DDL
                        " AND SEG_OWNER = '"+ schemaName.toUpperCase() +"' " +
                        buildTableInPredicate(whiteListTableNames) +
//                        " (commit_scn >= ? " +
                " AND SCN > ? AND SCN <= ? " +
                //" OR (OPERATION_CODE IN (7,36) AND USERNAME ='"+schemaName.toUpperCase()+"')";
        " OR (OPERATION_CODE IN (7,36) AND USERNAME NOT IN ('SYS','SYSTEM','"+logMinerUser.toUpperCase()+"'))"; //todo username = schemaName?
    }

    /**
     * This returns statement to query log miner view, pre-built for archived log files
     * @param schemaName user name
     * @return query
     */
    static String queryLogMinerArchivedContents(String schemaName)  {
        return "SELECT SCN, COMMIT_SCN, OPERATION, USERNAME, SRC_CON_NAME, SQL_REDO, SEG_TYPE, " +
                        "STATUS, OPERATION_CODE, TABLE_NAME, TIMESTAMP, COMMIT_TIMESTAMP, XID, XIDSQN " +
                        "FROM v$logmnr_contents " +
                        "WHERE " +
                        "username = '"+ schemaName.toUpperCase() +"' " +
//                        " AND OPERATION_CODE in (1,2,3,5,7, 36) " +// 5 - DDL
                        "AND seg_owner = '"+ schemaName.toUpperCase() +"'";
    }

    /**
     * After mining archived log files, we should remove them from the analysis.
     * NOTE. It does not physically remove the log file.
     * @param logFileName file ro remove
     * @return statement
     */
    static String getRemoveLogFileFromMiningStatement(String logFileName) {
        return "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE('" + logFileName + "'); END;";
    }

    static String getAddLogFileStatement(String option, String fileName) {
        return "BEGIN sys." +
                "dbms_logmnr.add_logfile(" +
                "LOGFILENAME => '" + fileName + "', " +
                "OPTIONS => " + option + ");" +
                "END;";
    }

    static String getRedoLogNameForScnQuery(Long scn){
        return "with get_ranked as " +
                "(SELECT F.MEMBER as member, r.first_change#, rank() over(order by r.first_change#  desc) as rank_num" +
                "FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# " +
                "and r.first_change# <= "+scn+")" +
                "select member from get_ranked where rank_num = 1";
    }

    /**
     * This method builds table_name IN predicate, filtering out non whitelisted tables from Log Mining.
     * This method limits joining over 1000 tables, Oracle will throw exception in such predicate.
     * @param tables white listed table names
     * @return IN predicate or empty string if number of whitelisted tables exceeds 1000
     */
    private static String buildTableInPredicate(List<String> tables) {
        if (tables.size() > 1000) {
            LOGGER.warn(" Cannot apply {} whitelisted tables condition", tables.size());
            return "";
        }
        StringJoiner tableNames = new StringJoiner(",");
        tables.forEach(table -> tableNames.add("'" + table + "'"));
        return " AND table_name IN (" + tableNames + ") AND SEG_NAME IN (" + tableNames + ") ";
    }
}
