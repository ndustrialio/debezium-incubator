/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;
    private final ErrorHandler errorHandler;
    private final TransactionalBuffer transactionalBuffer;
    private final OracleDmlParser dmlParser;
    private final String catalogName;
    //private final int posVersion;
    private OracleConnectorConfig connectorConfig;
    private final TransactionalBufferMetrics transactionalBufferMetrics;
    private final LogMinerMetrics logMinerMetrics;
    private final OracleConnectorConfig.LogMiningStrategy strategy;
    private final boolean isContinuousMining;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext,
                                              OracleConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema,
                                              OracleTaskContext taskContext) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = connectorConfig.getTablenameCaseInsensitive();
        //this.posVersion = connectorConfig.getOracleVersion().getPosVersion();
        OracleChangeRecordValueConverter converters = new OracleChangeRecordValueConverter(jdbcConnection);

        this.connectorConfig = connectorConfig;

        this.dmlParser = new OracleDmlParser(true, connectorConfig.getDatabaseName(), connectorConfig.getSchemaName(),
                converters);
        this.errorHandler = errorHandler;
        this.catalogName = (connectorConfig.getPdbName() != null) ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName();
        this.transactionalBufferMetrics = new TransactionalBufferMetrics(taskContext);
        this.transactionalBufferMetrics.register(LOGGER);
        transactionalBuffer = new TransactionalBuffer(connectorConfig.getLogicalName(), errorHandler, transactionalBufferMetrics);

        this.logMinerMetrics = new LogMinerMetrics(taskContext);
        this.logMinerMetrics.register(LOGGER);
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
    }

    // todo continuous_mine option, add redos only between offset and current
    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context change event source context
     * @throws InterruptedException an exception
     */
    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        Metronome metronome = Metronome.sleeper(Duration.ofMillis(1000L), clock);
        ResultSet res = null;

        try (Connection connection = jdbcConnection.connection();
             PreparedStatement fetchChangesFromMiningView =
                     connection.prepareStatement(SqlUtils.queryLogMinerContents(connectorConfig.getSchemaName(), jdbcConnection.username(), schema))) {
            long lastProcessedScn = offsetContext.getScn();
            long oldestScnInOnlineRedo = LogMinerHelper.getFirstOnlineLogScn(connection);
            if (lastProcessedScn < oldestScnInOnlineRedo) { // todo why this does not work?
                throw new RuntimeException("Online REDO LOG files don't contain the offset SCN. Clean offset info and start over");
            }


            // 1. Configure Log Miner to mine online redo logs
            LogMinerHelper.setNlsSessionParameters(jdbcConnection);

            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
            }
            LogMinerHelper.setSupplementalLogging(connection, schema.tableIds());

            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.resetSessionToCdb();
            }

            LOGGER.debug("strategy = {}", strategy.getValue());
            if (strategy == OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO) {
                LogMinerHelper.buildDataDictionary(connection);
            }
//            LogMinerHelper.addOnlineRedoLogFilesForMining(connection);
            List<String> filesToMine = new ArrayList<>();
            if (!isContinuousMining) {
                 filesToMine = LogMinerHelper.setRedoLogFilesForMining(connection, lastProcessedScn, new ArrayList<>());
            }
            LogMinerHelper.updateLogMinerMetrics(connection, logMinerMetrics);
            String currentRedoLogFile = LogMinerHelper.getCurrentRedoLogFile(connection, logMinerMetrics);

            // 2. Querying LogMinerRowLcr(s) from Log miner while running
            while (context.isRunning()) {
                LOGGER.trace("Receiving a change from LogMiner");

                long nextScn = LogMinerHelper.getNextScn(connection, lastProcessedScn, logMinerMetrics);
                LOGGER.debug("lastProcessedScn: {}, endScn: {}", lastProcessedScn, nextScn);

                String possibleNewCurrentLogFile = LogMinerHelper.getCurrentRedoLogFile(connection, logMinerMetrics);

                if (!currentRedoLogFile.equals(possibleNewCurrentLogFile)) {
                    LOGGER.debug("\n\n*****SWITCH occurred*****\n\n");
                    if (!isContinuousMining) {
                        if (strategy == OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO) {
                            // Oracle does the switch on building data dictionary in redo logs
                            LogMinerHelper.endMining(connection);
                            LogMinerHelper.buildDataDictionary(connection);
                        }
                        filesToMine = LogMinerHelper.setRedoLogFilesForMining(connection, lastProcessedScn, filesToMine);
                    }
                    LogMinerHelper.updateLogMinerMetrics(connection, logMinerMetrics);
                    currentRedoLogFile = LogMinerHelper.getCurrentRedoLogFile(connection, logMinerMetrics);
                }

                LogMinerHelper.startOnlineMining(connection, lastProcessedScn, nextScn, strategy, isContinuousMining);

                fetchChangesFromMiningView.setLong(1, lastProcessedScn);
                fetchChangesFromMiningView.setLong(2, nextScn);

                //traceInfo(connection, "before fetching changes");
                Instant startTime = Instant.now();
                res = fetchChangesFromMiningView.executeQuery();
                logMinerMetrics.setLastFetchingQueryDuration(Duration.between(startTime, Instant.now()));
                traceInfo(connection, "after fetching changes");

                if (res.isBeforeFirst()) {
                    processResult(res, context, logMinerMetrics);
                } else {
                    metronome.pause();
                }
                // update SCN in offset context only if buffer is empty
                if (transactionalBuffer.isEmpty()) {
                    offsetContext.setScn(nextScn);
                }
                lastProcessedScn = nextScn;
                res.close();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            // 3. disconnect
            if (transactionalBuffer != null) {
                transactionalBuffer.close();
                LOGGER.info("Transactional metrics dump: {}", transactionalBufferMetrics.toString());
                transactionalBufferMetrics.reset();
                transactionalBufferMetrics.unregister(LOGGER);
            }
            if (logMinerMetrics != null){
                LOGGER.info("LogMiner metrics dump: {}", logMinerMetrics.toString());
                logMinerMetrics.reset();
                logMinerMetrics.unregister(LOGGER);
            }
            try (Connection connection = jdbcConnection.connection()) {
                LogMinerHelper.endMining(connection);
            } catch (SQLException e) {
                LOGGER.error("Cannot borrow connection", e.getMessage());
            }

            if (res != null) {
                try {
                    res.close();
                } catch (SQLException e) {
                    LOGGER.error("Cannot close result set due to the :{}", e.getMessage());
                }
            }
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                LOGGER.error("Cannot close JDBC connection: {}", e.getMessage());
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // todo nothing here?
    }

    private void processResult(ResultSet res, ChangeEventSourceContext context, LogMinerMetrics metrics) throws SQLException {
        int counter = 0;
        Duration cumulativeCommitTime = Duration.ZERO;
        Duration cumulativeParseTime = Duration.ZERO;
        Duration cumulativeOtherTime = Duration.ZERO;
        Instant startTime = Instant.now();
        while (res.next()) {

            Instant iterationStart = Instant.now();

            BigDecimal scn = RowMapper.getScn(res);
            BigDecimal commitScn = RowMapper.getCommitScn(res);
            String operation = RowMapper.getOperation(res);
            String userName = RowMapper.getUserName(res);
            String redo_sql = RowMapper.getSqlRedo(res);
            int operationCode = RowMapper.getOperationCode(res);
            String tableName = RowMapper.getTableName(res);
            Timestamp changeTime = RowMapper.getChangeTime(res);
            String txId = RowMapper.getTransactionId(res);
            String segOwner = RowMapper.getSegOwner(res);
            String segName = RowMapper.getSegName(res);
            int sequence = RowMapper.getSequence(res);

            String logMessage = String.format("transactionId = %s, actualScn= %s, committed SCN= %s, userName= %s,segOwner=%s, segName=%s, sequence=%s",
                                    txId, scn, commitScn, userName, segOwner, segName, sequence);

            // Commit
            if (operationCode == RowMapper.COMMIT) {
                LOGGER.trace("COMMIT, {}", logMessage);
                transactionalBuffer.commit(txId, changeTime, context);
                cumulativeCommitTime = cumulativeCommitTime.plus(Duration.between(iterationStart, Instant.now()));
                continue;
            }

            //Rollback
            if (operationCode == RowMapper.ROLLBACK) {
                LOGGER.trace("ROLLBACK, {}", logMessage);
                transactionalBuffer.rollback(txId);
                continue;
            }

            if (tableName == null) {
                LOGGER.debug("table = null still happening");
                continue;
            }

            // DDL
            if (operationCode == RowMapper.DDL) {
                LOGGER.debug("DDL,  {}", logMessage);
                continue;
                // todo parse, add to the collection.
            }

            // DML
            if (operationCode == RowMapper.INSERT || operationCode == RowMapper.DELETE || operationCode == RowMapper.UPDATE) {
                LOGGER.trace("DML,  {}, sql {}", logMessage, redo_sql);
                counter++;
                try {
                    iterationStart = Instant.now();
                    dmlParser.parse(redo_sql, schema.getTables());
                    cumulativeParseTime = cumulativeParseTime.plus(Duration.between(iterationStart, Instant.now()));
                    iterationStart = Instant.now();

                    LogMinerRowLcr rowLcr = dmlParser.getDmlChange();
                    if (rowLcr == null) {
                        LOGGER.error("Following statement was not parsed: {}", redo_sql);
                        continue;
                    }
                    rowLcr.setObjectOwner(userName);
                    rowLcr.setSourceTime(changeTime);
                    rowLcr.setTransactionId(txId);
                    rowLcr.setObjectName(tableName);
                    rowLcr.setActualCommitScn(commitScn);
                    rowLcr.setActualScn(scn);
                    TableId tableId = RowMapper.getTableId(catalogName, res);

                    transactionalBuffer.registerCommitCallback(txId, scn, changeTime.toInstant(), (timestamp, smallestScn) -> {
                        // update SCN in offset context only if processed SCN less than SCN among other transactions
                        if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                            offsetContext.setScn(scn.longValue());
                        }
                        offsetContext.setTransactionId(txId);
                        offsetContext.setSourceTime(timestamp.toInstant());
                        offsetContext.setTableId(tableId);
                        Table table = schema.tableFor(tableId);
                        LOGGER.trace("Processing DML event {} scn {}", rowLcr.toString(), scn);
                        dispatcher.dispatchDataChangeEvent(tableId,
                                new LogMinerChangeRecordEmitter(offsetContext, rowLcr, table, clock)
                        );
                    });
                    cumulativeOtherTime = cumulativeOtherTime.plus(Duration.between(iterationStart, Instant.now()));

                } catch (Exception e) {
                    LOGGER.error("Following statement: {} cannot be parsed due to the : {}", redo_sql, e.getMessage());
                }
            }
        }
        metrics.setProcessedCapturedBatchDuration(Duration.between(startTime, Instant.now()));
        metrics.setCapturedDmlCount(counter);
        LOGGER.debug("{} DMLs were processes in {} milliseconds, commit time:{}, parse time:{}, other time:{}",
                counter, (Duration.between(startTime, Instant.now()).toMillis()),
                cumulativeCommitTime.toMillis(), cumulativeParseTime.toMillis(),
                cumulativeOtherTime.toMillis());
    }

    // todo this is temporary debugging info,  remove.
    private void traceInfo(Connection connection, String info) throws SQLException {
        String checkQuery = "select min(f.member), log.first_change#, log.sequence# from v$log log, v$logfile f  " +
                "where log.group#=f.group# and log.status='CURRENT' group by log.first_change#,log.sequence#";

        PreparedStatement st = connection.prepareStatement(checkQuery);
        ResultSet result = st.executeQuery();
        while (result.next()) {
            String fileName = result.getString(1);
            long changeScn = result.getLong(2);
            String sequence = result.getString(3);
            LOGGER.debug(info + "-> filename: {} ,first SCN: {}, sequence: {}", fileName, changeScn , sequence );
        }
        st.close();
        result.close();
    }
}
