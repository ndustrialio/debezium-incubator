/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class contains methods to configure and manage Log Miner utility
 */
public class LogMinerHelper {

    private final static Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    /**
     * This builds data dictionary objects in redo log files.
     * During this build, Oracle does an additional REDO LOG switch.
     * This call may take time, which leads to delay in delivering incremental changes.
     * With this option the lag between source database and dispatching event fluctuates.
     *
     * @param connection connection to the database as log miner user (connection to the container)
     * @throws SQLException any exception
     */
    static void buildDataDictionary(Connection connection) throws SQLException {
        executeCallableStatement(connection, SqlUtils.BUILD_DICTIONARY);
    }

    /**
     * This method returns current SCN from the database
     *
     * @param connection container level database connection
     * @return current SCN
     * @throws SQLException if anything unexpected happens
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
     * This method returns next SCN for mining  and also updates MBean metrics
     * We use a configurable limit, because the larger mining range, the slower query from Log Miner content view.
     * Gradual querying helps to catch up faster after long delays in mining.
     *
     * @param connection container level database connection
     * @param metrics MBean accessible metrics
     * @param lastProcessesScn offset SCN
     * @return next SCN to mine to
     * @throws SQLException if anything unexpected happens
     */
    static long getNextScn(Connection connection, long lastProcessesScn, LogMinerMetrics metrics) throws SQLException {
        long currentScn = getCurrentScn(connection);
        metrics.setCurrentScn(currentScn);
        int miningDiapason = metrics.getMaxBatchSize();
        return currentScn < (lastProcessesScn + miningDiapason) ? currentScn : lastProcessesScn + miningDiapason;
    }

    /**
     * This is to update MBean metrics
     * @param connection connection
     * @param metrics current metrics
     */
    static void updateLogMinerMetrics(Connection connection, LogMinerMetrics metrics) {
        try {
            // update metrics
            Map<String, String> logStatuses = getRedoLogStatus(connection);
            metrics.setRedoLogStatus(logStatuses);

            int counter = getSwitchCount(connection);
            metrics.setSwitchCount(counter);
        } catch (SQLException e) {
            LOGGER.error("Cannot update metrics");
        }
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
     * @param strategy this is about dictionary location
     * @param isContinuousMining works < 19 version only
     * @throws SQLException if anything unexpected happens
     */
    static void startOnlineMining(Connection connection, Long startScn, Long endScn,
                                         OracleConnectorConfig.LogMiningStrategy strategy, boolean isContinuousMining) throws SQLException {
        String statement = SqlUtils.getStartLogMinerStatement(startScn, endScn, strategy, isContinuousMining);
        executeCallableStatement(connection, statement);
        // todo dbms_logmnr.STRING_LITERALS_IN_STMT?
        // todo If the log file is corrupted/bad, logmnr will not be able to access it, we have to switch to another one?
    }

    /**
     * This method query the database to get CURRENT online redo log file
     * @param connection connection to reuse
     * @param metrics MBean accessible metrics
     * @return full redo log file name, including path
     * @throws SQLException if anything unexpected happens
     */
    static String getCurrentRedoLogFile(Connection connection, LogMinerMetrics metrics) throws SQLException {
        String checkQuery = SqlUtils.CURRENT_REDO_LOG_NAME;

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
     * @throws SQLException if anything unexpected happens
     */
    static long getFirstOnlineLogScn(Connection connection) throws SQLException {
        LOGGER.trace("getting first scn of all online logs");
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery(SqlUtils.OLDEST_FIRST_CHANGE);
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        res.close();
        return firstScnOfOnlineLog;
    }

    /**
     * Sets NLS parameters for mining session.
     *
     * @param connection session level database connection
     * @throws SQLException if anything unexpected happens
     */
    static void setNlsSessionParameters(JdbcConnection connection) throws SQLException {
        connection.executeWithoutCommitting(SqlUtils.NLS_SESSION_PARAMETERS);
    }

    /**
     * This fetches online redo log statuses
     * @param connection privileged connection
     * @return REDO LOG statuses Map, where key is the REDO name and value is the status
     * @throws SQLException if anything unexpected happens
     */
    private static Map<String, String> getRedoLogStatus(Connection connection) throws SQLException {
        return getMap(connection, SqlUtils.REDO_LOGS_STATUS, "unknown");
    }

    /**
     * This fetches REDO LOG switch count for the last day
     * @param connection privileged connection
     * @return counter
     */
    private static int getSwitchCount(Connection connection) {
        try {
            Map<String, String> total = getMap(connection, SqlUtils.SWITCH_HISTORY_TOTAL_COUNT, "unknown");
            if (total != null && total.get("total") != null){
                return Integer.parseInt(total.get("total"));
            }
        } catch (Exception e) {
            LOGGER.error("Cannot get switch counter due to the {}", e);
        }
        return 0;
    }

    /**
     * After a switch, we should remove it from the analysis.
     * NOTE. It does not physically remove the log file.
     *
     * @param logFileName file to delete from the analysis
     * @param connection  container level database connection
     * @throws SQLException if anything unexpected happens
     */
    private static void removeLogFileFromMining(String logFileName, Connection connection) throws SQLException {
        String removeLogFileFromMining = SqlUtils.getRemoveLogFileFromMiningStatement(logFileName);
        executeCallableStatement(connection, removeLogFileFromMining);
        LOGGER.debug("{} was removed from mining session", removeLogFileFromMining);

    }

    /**
     * This method checks if supplemental logging was set on the database level. If so it just logs this info.
     * If database level supplemental logging was not set, the method checks if each table has it and set it.
     * @param jdbcConnection oracle connection on logminer level
     * @param connection conn
     * @param pdbName pdb name
     * @param tableIds whitelisted tables
     * @throws SQLException if anything unexpected happens
     */
    static void setSupplementalLoggingForWhitelistedTables(OracleConnection jdbcConnection, Connection connection, String pdbName,
                                                                  Set<TableId> tableIds) throws SQLException {
        if (pdbName != null) {
            jdbcConnection.setSessionToPdb(pdbName);
        }

        final String key = "KEY";
        String validateGlobalLogging = "SELECT '" + key + "', " + " SUPPLEMENTAL_LOG_DATA_ALL from V$DATABASE";
        Map<String, String> globalLogging = getMap(connection, validateGlobalLogging, "unknown");
        if ("no".equalsIgnoreCase(globalLogging.get(key))) {
            tableIds.forEach(table -> {
                String tableName = table.schema() + "." + table.table();
                try {
                    String validateTableLevelLogging = String.format("SELECT '%s', LOG_GROUP_TYPE FROM DBA_LOG_GROUPS WHERE LOG_GROUP_TYPE='ALL COLUMN LOGGING' AND OWNER ='%s' AND TABLE_NAME = '%s'", key,
                            table.schema().toUpperCase(), table.table().toUpperCase());
                    Map<String, String> tableLogging = getMap(connection, validateTableLevelLogging, "unknown");
                    if (tableLogging.get(key) == null) {
                        String alterTableStatement = "ALTER TABLE " + tableName + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS";
                        LOGGER.info("altering table {} for supplemental logging", table.table());
                        executeCallableStatement(connection, alterTableStatement);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Cannot set supplemental logging for table " + tableName, e);
                }
            });
        } else {
            LOGGER.warn("Supplemental logging is set on global level, setting individual table supplemental logging was skipped");
        }

        if (pdbName != null) {
            jdbcConnection.resetSessionToCdb();
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
                LOGGER.error("Cannot close Log Miner session gracefully: {}", e);
            }
        }
    }

    /**
     * This method substitutes CONTINUOUS_MINE functionality for online files only
     * @param connection connection
     * @param lastProcessedScn current offset
     * @throws SQLException if anything unexpected happens
     */
    static void setRedoLogFilesForMining(Connection connection, Long lastProcessedScn) throws SQLException {

        Map<String, Long> logFilesForMining = getLogFilesForOffsetScn(connection, lastProcessedScn);
        if (logFilesForMining.isEmpty()) {
            throw new IllegalStateException("The online log files do not contain offset SCN, re-snapshot is required.");
        }

        List<String> logFilesNamesForMining = logFilesForMining.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());

        for (String file : logFilesNamesForMining) {
            String addLogFileStatement = SqlUtils.getAddLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
            LOGGER.debug("add log file to the mining session = {}", file);
        }

        LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesForMining);
    }

    /**
     * This method returns SCN as a watermark to abandon long lasting transactions.
     * This is a way to mitigate long lasting transactions when it could fall out of online redo logs range
     *
     * @param connection connection
     * @param lastProcessedScn current offset
     * @return Optional last SCN in a redo log
     * @throws SQLException if anything unexpected happens
     */
    static Optional<Long> getLastScnFromTheOldestOnlineRedo(Connection connection, Long lastProcessedScn) throws SQLException {
        Map<String, String> allOnlineRedoLogFiles = getMap(connection, SqlUtils.ALL_ONLINE_LOGS, "-1");

        Map<String, Long> logFilesToMine = getLogFilesForOffsetScn(connection, lastProcessedScn);
        LOGGER.debug("Redo log size = {}, needed for mining files size = {}", allOnlineRedoLogFiles.size(), logFilesToMine.size());

        if (allOnlineRedoLogFiles.size() - logFilesToMine.size() <= 1){
            List<Long> lastScnInOldestOnlineRedo = logFilesToMine.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
            return lastScnInOldestOnlineRedo.stream().min(Long::compareTo);
        }
        return Optional.empty();
    }


    /**
     * This method returns all online log files, starting from one which contains offset SCN and ending with one containing largest SCN
     * 18446744073709551615 on Ora 19c is the max value of the nextScn in the current redo todo replace all Long with BigDecimal for SCN
     */
    private static Map<String, Long> getLogFilesForOffsetScn(Connection connection, Long offsetScn) throws SQLException {
        Map<String, String> redoLogFiles = getMap(connection, SqlUtils.ALL_ONLINE_LOGS, "-1");
        return redoLogFiles.entrySet().stream().
                filter(entry -> new BigDecimal(entry.getValue()).longValue() > offsetScn || new BigDecimal(entry.getValue()).longValue() == -1).
                collect(Collectors.toMap(Map.Entry::getKey, e -> new BigDecimal(e.getValue()).longValue() == -1 ? Long.MAX_VALUE : new BigDecimal(e.getValue()).longValue()));
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
        try (
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String value = rs.getString(2);
                value = value == null ? nullReplacement : value;
                result.put(rs.getString(1), value);
            }
            return result;
        }
    }

}
