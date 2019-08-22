/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * This class contains methods to configure and manage Log Miner utility
 */
public class LogMinerHelper {

    private final static Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    /**
     * This builds data dictionary objects in redo log files. This is the first step of the LogMiner configuring.
     *
     * @param connection connection to the database as log miner user (connection to the container)
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void buildDataDictionary(Connection connection) throws SQLException {
        executeCallableStatement(connection, SqlUtils.BUILD_DICTIONARY);
    }

    /**
     * This method returns current SCN from the database
     *
     * @param connection container level database connection
     * @return current SCN
     * @throws SQLException fatal exception, cannot continue further
     */
    public static long getCurrentScn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(SqlUtils.CURRENT_SCN)) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            long currentScn = rs.getLong(1);
            rs.close();
            return currentScn;
        }
    }

    /**
     * This method returns current SCN from the database and updates MBean metrics
     *
     * @param connection container level database connection
     * @param metrics MBean accessible metrics
     * @param lastProcessesScn offset SCN
     * @return next SCN to mine to
     * @throws SQLException fatal exception, cannot continue further
     */
    public static long getNextScn(Connection connection, long lastProcessesScn, LogMinerMetrics metrics) throws SQLException {
        long currentScn = getCurrentScn(connection);
        metrics.setCurrentScn(currentScn);
        int miningDiapason = metrics.getMaxMiningBatchSize();
        return currentScn < lastProcessesScn + miningDiapason ? currentScn : lastProcessesScn + miningDiapason;
    }

    /**
     * This adds all online redo log files for mining.
     *
     * @param connection container level database connection
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void addOnlineRedoLogFilesForMining(Connection connection) throws SQLException {

        // add files for mining
        Map<String, String> logFiles = getLogFileNames(connection);
        Set<String> fileNames = logFiles.keySet();
        if (fileNames.size() < 3) {
            LOGGER.warn("Number of redo log files {} is insufficient for efficient Log Mining", fileNames.size());
        }

        for (String fileName : fileNames) {
            String addLogFileStatement = SqlUtils.getAddLogFileStatement("DBMS_LOGMNR.ADDFILE", fileName); // todo simplify
            LOGGER.debug("log file = {}", fileName);
            executeCallableStatement(connection, addLogFileStatement);
        }
    }

    /**
     * todo
     * @param connection
     * @param metrics
     * @throws SQLException
     */
    public static void updateLogMinerMetrics(Connection connection, LogMinerMetrics metrics) throws SQLException {
        // update metrics
        Map<String, String> logStatuses = getRedoLogStatus(connection);
        metrics.setRedoLogState(logStatuses);

        Map<String, String> history = getSwitchHistory(connection);
        metrics.setSwitchHistory(history);

        Map<String, String> logFileStatuses = getRedoLogFileStatus(connection);
        metrics.setRedoLogFileStatuses(logFileStatuses);

        Map<String, String> logFileSequences = getRedoLogSequence(connection);
        metrics.setRedoLogSequences(logFileSequences);
    }

    /**
     * This method builds mining view to query changes from.
     * This view is built for online redo log files.
     * It starts log mining session.
     * It uses data dictionary objects, incorporated in previous steps.
     * It tracks DDL changes and mines committed data only.
     *
     * @param connection container level database connection
     * @param startScn   the SCN to mine from
     * @param endScn     the SCN to mine to
     * PARAM todo
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void startOnlineMining(Connection connection, Long startScn, Long endScn,
                                         OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining) throws SQLException {
        String statement = SqlUtils.getStartLogMinerStatement(startScn, endScn, strategy, isContinuousMining);
        executeCallableStatement(connection, statement);
        // todo dbms_logmnr.STRING_LITERALS_IN_STMT?
        // todo If the log file is corrupter bad, logmnr will not be able to access it, what to do?
    }

    /**
     * This method query the database to get CURRENT online redo log file
     * @param connection connection to reuse
     * @param metrics MBean accessible metrics
     * @return full redo log file name, including path
     * @throws SQLException this would be something fatal
     */
    public static String getCurrentRedoLogFile(Connection connection, LogMinerMetrics metrics) throws SQLException {
        String checkQuery = "select f.member " +
                "from v$log log, v$logfile f  where log.group#=f.group# and log.status='CURRENT'";

        String fileName = "";
        PreparedStatement st = connection.prepareStatement(checkQuery);
        ResultSet result = st.executeQuery();
        while (result.next()) {
            fileName = result.getString(1);
            LOGGER.trace(" Current Redo log fileName: {} ",  fileName);
        }
        st.close();
        result.close();

        metrics.setCurrentLogFileName(fileName);
        return fileName;
    }

    /**
     * This method fetches the oldest SCN from online redo log files
     *
     * @param connection container level database connection
     * @return oldest SCN from online redo log
     * @throws SQLException fatal exception, cannot continue further
     */
    public static long getFirstOnlineLogScn(Connection connection) throws SQLException {
        LOGGER.debug("getting first scn of online log");
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery(SqlUtils.OLDEST_FIRST_CHANGE);
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        res.close();
        return firstScnOfOnlineLog;
    }

    /**
     * This method is fetching changes from a batch of archived log files
     *
     * @param conn                 container level database connection
     * @param batchSize            number of archived files to mine at once
     * @param schemaName           user that made schema changes
     * @param archivedFileIterator Iterator for archived files
     * @return ResultSet result
     * @throws SQLException fatal exception, cannot continue further
     */
    public static ResultSet getArchivedChanges(Connection conn, int batchSize, String schemaName, Iterator<Map.Entry<String, Long>> archivedFileIterator) throws SQLException {

        while (batchSize > 0 && archivedFileIterator.hasNext()) {
            String name = archivedFileIterator.next().getKey();
            addRedoLogFileForMining(conn, name);
            LOGGER.debug("{} was added for mining", archivedFileIterator);
            batchSize--;
        }

        buildArchivedMiningView(conn);

        // mining
        PreparedStatement getChangesQuery = conn.prepareStatement(SqlUtils.queryLogMinerArchivedContents(schemaName));
        ResultSet result = getChangesQuery.executeQuery();
        getChangesQuery.close();
        return result;
    }

    /**
     * This method fetches all archived log files names, starting from the one which contains offset SCN.
     * This is needed what the connector is down for a long time and some changes were archived.
     * This will help to catch up old changes during downtime.
     *
     * @param offsetScn  offset SCN
     * @param connection container level database connection
     * @return java.util.Map as List of archived log filenames and it's next_change# field value
     * @throws SQLException fatal exception, cannot continue further
     */
    public static Map<String, Long> getArchivedLogFiles(long offsetScn, Connection connection) throws SQLException {
        // this happens for the first connection start. No offset yet, we should capture current changes only.
        if (offsetScn == -1) {
            return Collections.emptyMap();
        }

        long oldestArchivedScn = getFirstArchivedLogScn(connection);
        if (offsetScn < oldestArchivedScn) {
            throw new SQLException("There are no log files containing this SCN. " +
                    "Most likely the connector was down for a long time and archived log files were purged");
        }

        Map<String, Long> allLogs = new TreeMap<>();

        PreparedStatement ps = connection.prepareStatement(SqlUtils.LATEST_SCN_FROM_ARCHIVED_LOG);
        ResultSet res = ps.executeQuery();
        res.next();
        long maxArchivedScn = res.getLong(1);

        ps = connection.prepareStatement(SqlUtils.ALL_ARCHIVED_LOGS_NAMES_FOR_OFFSET);
        ps.setLong(1, offsetScn);
        ps.setLong(2, maxArchivedScn);
        res = ps.executeQuery();
        while (res.next()) {
            allLogs.put(res.getString(1), res.getLong(2));
            LOGGER.info("Log file to mine: {}, next change = {} ", res.getString(1), res.getLong(2));
        }
        ps.close();
        res.close();
        return allLogs;
    }

    /**
     * After a switch, we should remove it from the analysis.
     * NOTE. It does not physically remove the log file.
     *
     * @param logFileName file to delete from the analysis
     * @param connection  container level database connection
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void removeLogFileFromMining(String logFileName, Connection connection) throws SQLException {
        String removeLogFileFromMining = SqlUtils.getRemoveLogFileFromMiningStatement(logFileName);
        executeCallableStatement(connection, removeLogFileFromMining);
        LOGGER.debug("{} was removed from mining", removeLogFileFromMining);

    }

    /**
     * Sets NLS parameters for mining session.
     *
     * @param connection session level database connection
     * @throws SQLException if anything unexpected happens
     */
    public static void setNlsSessionParameters(JdbcConnection connection) throws SQLException {
        connection.executeWithoutCommitting(SqlUtils.NLS_SESSION_PARAMETERS);
    }

    /**
     * This finds redo log, containing given SCN
     * @param connection privileged connection
     * @param scn the SCN
     * @return fine name
     * @throws SQLException if anything unexpected happens
     */
    public static String getRedoLogForScn(Connection connection, Long scn) throws SQLException {
        String fileName = "no online REDO file contains SCN: " + scn;
        if (scn != null && scn > 0) {
            String query = SqlUtils.getRedoLogNameForScnQuery(scn);
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                fileName = rs.getString(1);
            }
            rs.close();
        }
        return fileName;
    }

    /**
     * This fetches online redo log statuses
     * @param connection privileged connection
     * @return REDO LOG statuses Map, where key is the REDO name and value is the status
     * @throws SQLException if anything unexpected happens
     */
    public static Map<String, String> getRedoLogStatus(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.REDO_LOGS_STATUS, "unknown");
    }

    /**
     * This fetches online redo log file statuses
     * @param connection privileged connection
     * @return REDO LOG FILE statuses Map, where key is the file name and value is the status
     * @throws SQLException if anything unexpected happens
     */
    public static Map<String, String> getRedoLogFileStatus(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.REDO_LOG_FILES_STATUS, "file in use");
    }

    /**
     * This fetches REDO LOG switch history for the last day
     * @param connection privileged connection
     * @return Map of switching history info, where KEY is file name the switch happened from and value - time of the day
     * @throws SQLException if anything unexpected happens
     */
    public static Map<String, String> getSwitchHistory(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.SWITCH_HISTORY, "unknown");
    }

    /**
     * This fetches REDO LOG file names and group number. It picks up only one member per group to avoid duplications.
     * @param connection privileged connection
     * @return Map of lgo file names , where KEY is file name and value - number of the group
     * @throws SQLException if anything unexpected happens
     */
    public static Map<String, String> getLogFileNames(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.ONLINE_LOG_FILENAME, "unknown");
    }

    /**
     * This fetches online redo log file sequences
     * @param connection privileged connection
     * @return REDO LOG FILE statuses Map, where key is the file name and value is the sequence
     * @throws SQLException if anything unexpected happens
     */
    public static Map<String, String> getRedoLogSequence(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.REDO_LOGS_SEQUENCE, "unknown");
    }

    /**
     * todo
     * @param tables
     */
    public static void setSupplementalLogging(Connection connection, Set<TableId> tables) throws SQLException{
        final String supplementalLogDataAll = "SUPPLEMENTAL_LOG_DATA_ALL";
        String validateLogging = "select 'SUPPLEMENTAL_LOG_DATA_ALL', " + supplementalLogDataAll + " from V$DATABASE";
        Map<String, String> logging = getMap(connection, validateLogging, "unknown");
        if ("NO".equalsIgnoreCase(logging.get(supplementalLogDataAll))) {
            tables.forEach(table -> {
                String tableName = table.schema() + "." + table.table();
                try {
                    String alterTableStatement = "ALTER TABLE " + tableName + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS";
                    executeCallableStatement(connection, alterTableStatement);
                    LOGGER.info("altering table {} for supplemental logging", table.table());
                } catch (SQLException e) {
                    LOGGER.warn("cannot do it twice for table {}", tableName);
                    //throw new RuntimeException("Cannot set supplemental logging for table " + tableName, e);
                }
            });
        } else {
            LOGGER.warn("Supplemental logging is set on global level, individual table supplemental logging was skipped");
        }
    }

    /**
     * This call completes log miner session.
     * Complete gracefully.
     *
     * @param connection container level database connection
     */
    static void endMining(Connection connection) {
        String stopMining = SqlUtils.END_LOGMNR;
        try {
            executeCallableStatement(connection, stopMining);
        } catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("Log Miner session was already closed");
            } else {
                LOGGER.error("Cannot close Log Miner session gracefully: {}", e.getMessage());
            }
        }
    }

    /**
     * This method fetches the oldest SCN from online redo log files
     *
     * @param connection container level database connection
     * @return oldest SCN from online redo log
     * @throws SQLException fatal exception, cannot continue further
     */
    private static long getFirstArchivedLogScn(Connection connection) throws SQLException {
        LOGGER.debug("getting first scn of archived log");
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery(SqlUtils.OLDEST_ARCHIVED_CHANGE);
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        s.close();
        res.close();
        return firstScnOfOnlineLog;
    }

    /**
     * A method to add log file for mining
     *
     * @param connection Database connection
     * @param fileName   Redo log file name
     * @throws SQLException fatal exception
     */
    public static void addRedoLogFileForMining(Connection connection, String fileName) throws SQLException {
        final String addLogFileStatement = SqlUtils.getAddLogFileStatement("DBMS_LOGMNR.ADDFILE", fileName);
        executeCallableStatement(connection, addLogFileStatement);
        LOGGER.debug("Redo log file= {} added for mining", fileName);
    }

    // todo
    public static List<String> setRedoLogFilesForMining(Connection connection, Long offsetScn, List<String> currentLogFilesForMining) throws SQLException {
        String query = SqlUtils.ALL_ONLINE_LOGS_NAMES_FOR_OFFSET; // todo filter in the query
        Map<String, String> logFiles = getMap(connection, query, "-1");
        // todo -1000
        List<String> logFilesForMining = logFiles.entrySet().stream().filter(entry -> Double.parseDouble(entry.getValue()) > offsetScn-1000).map(Map.Entry::getKey).collect(Collectors.toList());
        LOGGER.debug("current list : {}, new list to mine: {}", currentLogFilesForMining, logFilesForMining);
        List<String> outdatedFiles = currentLogFilesForMining.stream().filter(file -> !logFilesForMining.contains(file)).collect(Collectors.toList());
        for (String file : outdatedFiles) {
            removeLogFileFromMining(file, connection);
        }
        List<String> filesToAddForMining = logFilesForMining.stream().filter(file -> !currentLogFilesForMining.contains(file)).collect(Collectors.toList());
        for (String file : filesToAddForMining) {
            String addLogFileStatement = SqlUtils.getAddLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            LOGGER.debug("log file added = {}", file);
            executeCallableStatement(connection, addLogFileStatement);
        }

        return logFilesForMining;
    }

    /**
     * This method builds mining view to query changes from.
     * This view is built for archived log files.
     * It uses data dictionary from online catalog. However DDL will be available.
     *
     * @param connection container level database connection
     * @throws SQLException fatal exception, cannot continue further
     */
    private static void buildArchivedMiningView(Connection connection) throws SQLException {
        executeCallableStatement(connection, SqlUtils.START_LOGMINER_FOR_ARCHIVE_STATEMENT);
        // todo If the archive is bad, logMiner will not be able to access it, maybe we should continue gracefully:
    }

    private static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        CallableStatement s;
        s = connection.prepareCall(statement);
        s.execute();
        s.close();
    }

    private static Map<String, String> getMap(Connection connection, String query, String nullReplacement) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            String value = rs.getString(2);
            value = value == null ? nullReplacement : value;
            result.put(rs.getString(1), value);
        }
        rs.close();
        return result;
    }

}
