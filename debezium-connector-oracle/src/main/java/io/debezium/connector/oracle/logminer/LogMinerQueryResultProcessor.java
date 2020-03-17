/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.jsqlparser.SimpleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * This class process entries obtained from LogMiner view.
 * It parses each entry.
 * On each DML it registers a callback in TransactionalBuffer.
 * On rollback it removes registered entries from TransactionalBuffer.
 * On commit it executes all registered callbacks, which dispatch ChangeRecords.
 * This also calculates metrics
 */
public class LogMinerQueryResultProcessor {

    private final ChangeEventSource.ChangeEventSourceContext context;
    private final LogMinerMetrics metrics;
    private final TransactionalBuffer transactionalBuffer;
    private final SimpleDmlParser dmlParser;
    private final OracleOffsetContext offsetContext;
    private final OracleDatabaseSchema schema;
    private final EventDispatcher<TableId> dispatcher;
    private final TransactionalBufferMetrics transactionalBufferMetrics;
    private final String catalogName;
    private final Clock clock;
    private final Logger LOGGER = LoggerFactory.getLogger(LogMinerQueryResultProcessor.class);

    public LogMinerQueryResultProcessor(ChangeEventSource.ChangeEventSourceContext context, LogMinerMetrics metrics,
                                        TransactionalBuffer transactionalBuffer, SimpleDmlParser dmlParser,
                                        OracleOffsetContext offsetContext, OracleDatabaseSchema schema,
                                        EventDispatcher<TableId> dispatcher, TransactionalBufferMetrics transactionalBufferMetrics,
                                        String catalogName, Clock clock) {
        this.context = context;
        this.metrics = metrics;
        this.transactionalBuffer = transactionalBuffer;
        this.dmlParser = dmlParser;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.transactionalBufferMetrics = transactionalBufferMetrics;
        this.catalogName = catalogName;
        this.clock = clock;
    }

    /**
     * This method does all the job
     * @param resultSet the info from Log Miner view
     * @return number of processed DMLs from the given resultSet
     */
    public int processResult(ResultSet resultSet) {
        int dmlCounter = 0;
        int commitCounter = 0;
        int rollbackCounter = 0;
        Duration cumulativeCommitTime = Duration.ZERO;
        Duration cumulativeRollbackTime = Duration.ZERO;
        Duration cumulativeParseTime = Duration.ZERO;
        Duration cumulativeOtherTime = Duration.ZERO;
        Instant startTime = Instant.now();
        while (true) {
            try {
                if (!resultSet.next()) {
                    break;
                }
            } catch (SQLException e) {
                RowMapper.logError(e, "Closed resultSet");
                return 0;
            }

            Instant iterationStart = Instant.now();

            BigDecimal scn = RowMapper.getScn(resultSet);
            String redo_sql = RowMapper.getSqlRedo(resultSet);
            String tableName = RowMapper.getTableName(resultSet);
            String segOwner = RowMapper.getSegOwner(resultSet);
            int operationCode = RowMapper.getOperationCode(resultSet);
            Timestamp changeTime = RowMapper.getChangeTime(resultSet);
            String txId = RowMapper.getTransactionId(resultSet);

            String logMessage = String.format("transactionId = %s, SCN= %s, table_name= %s, segOwner= %s, operationCode=%s, offsetSCN= %s",
                    txId, scn, tableName, segOwner, operationCode, offsetContext.getScn());

            if (scn == null) {
                LOGGER.warn("Scn is null for {}", logMessage);
                return 0;
            }

            // Commit
            if (operationCode == RowMapper.COMMIT) {
                if (transactionalBuffer.commit(txId, changeTime, context, logMessage)){
                    LOGGER.trace("COMMIT, {}", logMessage);
                    commitCounter++;
                    cumulativeCommitTime = cumulativeCommitTime.plus(Duration.between(iterationStart, Instant.now()));
                }
                continue;
            }

            //Rollback
            if (operationCode == RowMapper.ROLLBACK) {
                if (transactionalBuffer.rollback(txId, logMessage)){
                    LOGGER.trace("ROLLBACK, {}", logMessage);
                    rollbackCounter++;
                    cumulativeRollbackTime = cumulativeRollbackTime.plus(Duration.between(iterationStart, Instant.now()));
                }
                continue;
            }

            // DDL
            if (operationCode == RowMapper.DDL) {
                LOGGER.debug("DDL: {}, REDO_SQL: {}", logMessage, redo_sql);
                continue;
                // todo parse, add to the collection.
            }

            // MISSING_SCN
            if (operationCode == RowMapper.MISSING_SCN) {
                LOGGER.warn("Missing SCN,  {}", logMessage);
                continue;
            }

            // DML
            if (operationCode == RowMapper.INSERT || operationCode == RowMapper.DELETE || operationCode == RowMapper.UPDATE) {
                LOGGER.trace("DML,  {}, sql {}", logMessage, redo_sql);
                dmlCounter++;
                iterationStart = Instant.now();
                dmlParser.parse(redo_sql, schema.getTables(), txId);
//                    dmlParser.parse(redo_sql, schema.getTables());
                cumulativeParseTime = cumulativeParseTime.plus(Duration.between(iterationStart, Instant.now()));
                iterationStart = Instant.now();

                LogMinerRowLcr rowLcr = dmlParser.getDmlChange();
                LOGGER.trace("parsed record: {}" , rowLcr);
                if (rowLcr == null || redo_sql == null) {
                    LOGGER.error("Following statement was not parsed: {}, details: {}", redo_sql, logMessage);
                    continue;
                }

                // this will happen for instance on a blacklisted column change, we will omit this update
                if (rowLcr.getCommandType().equals(Envelope.Operation.UPDATE)
                        && rowLcr.getOldValues().size() == rowLcr.getNewValues().size()
                        && rowLcr.getNewValues().containsAll(rowLcr.getOldValues())) {
                    LOGGER.trace("Following DML was skipped, " +
                            "most likely because of ignored blacklisted column change: {}, details: {}", redo_sql, logMessage);
                    continue;
                }

                rowLcr.setObjectOwner(segOwner);
                rowLcr.setSourceTime(changeTime);
                rowLcr.setTransactionId(txId);
                rowLcr.setObjectName(tableName);
                rowLcr.setScn(scn);

                try {
                    TableId tableId = RowMapper.getTableId(catalogName, resultSet);

                    transactionalBuffer.registerCommitCallback(txId, scn, changeTime.toInstant(), redo_sql, (timestamp, smallestScn) -> {
                        // update SCN in offset context only if processed SCN less than SCN among other transactions
                        if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                            offsetContext.setScn(scn.longValue());
                            transactionalBufferMetrics.setOldestScn(scn.longValue());
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
                    LOGGER.error("Following rowLcr: {} cannot be dispatched due to the : {}", rowLcr, e);
                }
            }
        }
        metrics.setProcessedCapturedBatchDuration(Duration.between(startTime, Instant.now()));
        metrics.setCapturedDmlCount(dmlCounter);
        if (dmlCounter > 0 || commitCounter > 0 || rollbackCounter > 0) {
            LOGGER.debug("{} DMLs, {} Commits, {} Rollbacks were processed in {} milliseconds, commit time:{}, rollback time: {}, parse time:{}, " +
                            "other time:{}, lag:{}, offset:{}",
                    dmlCounter, commitCounter, rollbackCounter, (Duration.between(startTime, Instant.now()).toMillis()),
                    cumulativeCommitTime.toMillis(), cumulativeRollbackTime.toMillis(),
                    cumulativeParseTime.toMillis(), cumulativeOtherTime.toMillis(),
                    transactionalBufferMetrics.getLagFromSource(), offsetContext.getScn());
        }
        return dmlCounter;
    }
}
