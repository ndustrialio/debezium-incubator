/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

/**
 * This interface is exposed for JMX
 */
public interface LogMinerMetricsMXBean {

    /**
     * Exposes current SCN in the database. This is very efficient query and will not affect overall performance
     *
     * @return current SCN
     */
    Long getCurrentScn();

    /**
     * Exposes current redo log file. This is very efficient query and will not affect overall performance
     *
     * @return full path or NULL if an exception occurs.
     */
    String getCurrentRedoLogFileName();

    /**
     * Exposes states of redo logs: current, active, inactive, unused ...
     * @return array of: (redo log name | status) elements
     */
    String[] getRedoLogStatus();

    /**
     * fetches history of redo switches for the 7 days todo
     * @return array of: (redo log file name | time of switch) elements
     */
    String[] getSwitchHistory();

    /**
     * @return number of milliseconds last Log Miner query took
     */
    Long getLastLogMinerQueryDuration();

    /**
     * @return number of captured DML since the connector is up
     */
    int getCapturedDmlCount();

    /**
     * @return number of Log Miner view queries since the connector is up
     */
    int getLogMinerQueryCount();

    /**
     * @return average duration of Log Miner view query
     */
    Long getAverageLogMinerQueryDuration();

    /**
     * Log Miner view query returns number of captured DML , Commit and Rollback. This is what we call a batch.
     * @return duration of the last batch processing, which includes parsing and dispatching
     */
    Long getLastProcessedCapturedBatchDuration();

    /**
     * Log Miner view query returns number of captured DML , Commit and Rollback. This is what we call a batch.
     * @return number of all processed batches , which includes parsing and dispatching
     */
    int getProcessedCapturedBatchCount();

    /**
     * todo Java doc
     * @return
     */
    Long getAverageProcessedCapturedBatchDuration();

    int getMaxBatchSize();

    void setMaxBatchSize(int size);

    int getMillisecondToSleepBetweenMiningQuery();

    void setMillisecondToSleepBetweenMiningQuery(int milliseconds);

    int getFetchedRecordSizeLimitToFallAsleep();

    void setFetchedRecordSizeLimitToFallAsleep(int size);

    boolean getCTAS();

    void setCTAS(boolean ctas);
}
