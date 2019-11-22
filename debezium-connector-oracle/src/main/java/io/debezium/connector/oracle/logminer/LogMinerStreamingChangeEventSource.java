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
import io.debezium.connector.oracle.jsqlparser.SimpleDmlParser;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    private final TransactionalBuffer transactionalBuffer;
    // todo introduce injection of appropriate parser
//    private final OracleDmlParser dmlParser;
    private final SimpleDmlParser dmlParser;
    private final String catalogName;
    //private final int posVersion;
    private OracleConnectorConfig connectorConfig;
    private final TransactionalBufferMetrics transactionalBufferMetrics;
    private final LogMinerMetrics logMinerMetrics;
    private final OracleConnectorConfig.LogMiningStrategy strategy;
    private final boolean isContinuousMining;
    private long lastProcessedScn;
    private long nextScn;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext,
                                              OracleConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema,
                                              OracleTaskContext taskContext) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        OracleChangeRecordValueConverter converters = new OracleChangeRecordValueConverter(jdbcConnection);

        this.connectorConfig = connectorConfig;

//        this.dmlParser = new OracleDmlParser(true, connectorConfig.getDatabaseName(), connectorConfig.getSchemaName(),
//                converters);
        this.dmlParser = new SimpleDmlParser(connectorConfig.getDatabaseName(), connectorConfig.getSchemaName(), converters);
        this.catalogName = (connectorConfig.getPdbName() != null) ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName();
        this.transactionalBufferMetrics = new TransactionalBufferMetrics(taskContext);
        this.transactionalBufferMetrics.register(LOGGER);
        transactionalBuffer = new TransactionalBuffer(connectorConfig.getLogicalName(), errorHandler, transactionalBufferMetrics);

        this.logMinerMetrics = new LogMinerMetrics(taskContext);
        this.logMinerMetrics.register(LOGGER);
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context change event source context
     * @throws InterruptedException an exception
     */
    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        Metronome metronome;

        try (Connection connection = jdbcConnection.connection();
             PreparedStatement fetchFromMiningView =
                     connection.prepareStatement(SqlUtils.
                             queryLogMinerContents(connectorConfig.getSchemaName(), jdbcConnection.username(), schema, SqlUtils.LOGMNR_CONTENTS_VIEW));
             PreparedStatement fetchFromMiningTable =
                     connection.prepareStatement(SqlUtils.
                             queryLogMinerContents(connectorConfig.getSchemaName(), jdbcConnection.username(), schema, SqlUtils.LOGMNR_CONTENTS_TABLE))) {

            lastProcessedScn = offsetContext.getScn();
            long oldestScnInOnlineRedo = LogMinerHelper.getFirstOnlineLogScn(connection);
            if (lastProcessedScn < oldestScnInOnlineRedo) {
                throw new RuntimeException("Online REDO LOG files don't contain the offset SCN. Clean offset and start over");
            }

            // 1. Configure Log Miner to mine online redo logs
            LogMinerHelper.setNlsSessionParameters(jdbcConnection);
            LogMinerHelper.setSupplementalLoggingForWhitelistedTables(jdbcConnection, connection, connectorConfig.getPdbName(), schema.tableIds());

            LOGGER.debug("Data dictionary catalog = {}", strategy.getValue());

            if (strategy == OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO) {
                LogMinerHelper.buildDataDictionary(connection);
            }

            List<String> filesToMine = new ArrayList<>();
            if (!isContinuousMining) {
                 LogMinerHelper.setRedoLogFilesForMining(connection, lastProcessedScn, filesToMine);
            }
            LogMinerHelper.updateLogMinerMetrics(connection, logMinerMetrics);
            String currentRedoLogFile = LogMinerHelper.getCurrentRedoLogFile(connection, logMinerMetrics);

            LogMinerQueryResultProcessor processor = new LogMinerQueryResultProcessor(context, logMinerMetrics, transactionalBuffer,
                    dmlParser, offsetContext, schema, dispatcher, transactionalBufferMetrics, catalogName, clock);

            // 2. Querying LogMiner view while running
            while (context.isRunning()) {
                LOGGER.trace("Receiving a change from LogMiner");
                metronome = Metronome.sleeper(Duration.ofMillis(logMinerMetrics.getMillisecondToSleepBetweenMiningQuery()), clock);

                nextScn = LogMinerHelper.getNextScn(connection, lastProcessedScn, logMinerMetrics);
                LOGGER.trace("lastProcessedScn: {}, endScn: {}", lastProcessedScn, nextScn);

                String possibleNewCurrentLogFile = LogMinerHelper.getCurrentRedoLogFile(connection, logMinerMetrics);

                if (!currentRedoLogFile.equals(possibleNewCurrentLogFile)) {
                    LOGGER.debug("\n\n***** SWITCH occurred *****\n" + " from:{} , to:{} \n\n", currentRedoLogFile, possibleNewCurrentLogFile);

                    // This is the way to mitigate PGA leak.
                    // With one mining session it grows and maybe there is another way to flush PGA, but at this point we use new session
                    LogMinerHelper.endMining(connection);

                    if (!isContinuousMining) {
                        filesToMine.clear();
                        if (strategy == OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO) {
                            LogMinerHelper.buildDataDictionary(connection);
                        }
                        LogMinerHelper.setRedoLogFilesForMining(connection, lastProcessedScn, filesToMine);
                    }

                    // Abandon long running transactions
                    Optional<Long> oldestScnToAbandonTransactions = LogMinerHelper.getLastScnFromTheOldestMiningRedo(connection, offsetContext.getScn());
                    oldestScnToAbandonTransactions.ifPresent(scn -> {
                        transactionalBuffer.abandonLongTransactions(scn);
                        offsetContext.setScn(scn);
                        try {
                            nextScn = LogMinerHelper.getNextScn(connection, lastProcessedScn, logMinerMetrics);
                            LogMinerHelper.setRedoLogFilesForMining(connection, lastProcessedScn, filesToMine);
                        } catch (SQLException e) {
                            LOGGER.error("Couldn't abandon transactions due to {}", e);
                        }
                        LOGGER.debug("offset before: {}, offset after:{}, next SCN:{}", offsetContext.getScn(), scn, nextScn);
                    });

                    LogMinerHelper.updateLogMinerMetrics(connection, logMinerMetrics); // todo : check if it slows down the iteration

                    currentRedoLogFile = LogMinerHelper.getCurrentRedoLogFile(connection, logMinerMetrics);
                }

                LogMinerHelper.startOnlineMining(connection, lastProcessedScn, nextScn, strategy, isContinuousMining);

                ResultSet res;
                Instant startTime = Instant.now();
                if (!logMinerMetrics.getCTAS()) {

                    fetchFromMiningView.setLong(1, lastProcessedScn);
                    fetchFromMiningView.setLong(2, nextScn);

                    res = fetchFromMiningView.executeQuery();
                } else {
                    LogMinerHelper.createTempTable(connection, connectorConfig.getSchemaName(), jdbcConnection.username(), lastProcessedScn); // todo not good, > 1.4 sec

                    fetchFromMiningTable.setLong(1, lastProcessedScn);
                    fetchFromMiningTable.setLong(2, nextScn);

                    res = fetchFromMiningTable.executeQuery();

                }
                logMinerMetrics.setLastLogMinerQueryDuration(Duration.between(startTime, Instant.now()));

                int processedCount = processor.processResult(res);

                if (processedCount < logMinerMetrics.getFetchedRecordSizeLimitToFallAsleep()) {
                    LOGGER.trace("sleeping for {} milliseconds", logMinerMetrics.getMillisecondToSleepBetweenMiningQuery());
                    metronome.pause();
                }

                // update SCN in offset context only if buffer is empty
                if (transactionalBuffer.isEmpty()) {
                    offsetContext.setScn(nextScn);
                }
                lastProcessedScn = nextScn; // todo abandoned logic?
                res.close();
            }
        } catch (Throwable e) {
            LOGGER.error("Mining session was stopped due to the {} ", e);
            throw new RuntimeException(e);
        } finally {
            LOGGER.info("lastProcessedScn={}, nextScn={}, offsetContext.getScn()={}", lastProcessedScn, nextScn, offsetContext.getScn());
            // 3. dump and end
            if (transactionalBuffer != null) {
                LOGGER.info("Transactional metrics dump: {}", transactionalBufferMetrics.toString());
                LOGGER.info("Transactional buffer dump: {}", transactionalBuffer.toString());
                transactionalBuffer.close();
                transactionalBufferMetrics.unregister(LOGGER);
            }
            if (logMinerMetrics != null){
                LOGGER.info("LogMiner metrics dump: {}", logMinerMetrics.toString());
                logMinerMetrics.unregister(LOGGER);
            }
            try (Connection connection = jdbcConnection.connection()) {
                LogMinerHelper.endMining(connection);
            } catch (SQLException e) {
                LOGGER.error("Connection exception happened", e.getMessage());
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }
}
