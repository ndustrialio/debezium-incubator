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
    String getCurrentRedoLogFile();

    /**
     * Exposes states of redo logs: current, active, inactive, unused ...
     * @return array of: (redo log name | status) elements
     */
    String[] getRedoLogStatuses();

    /**
     * Exposes states of redo logs files: valid, invalid
     * @return array of: (redo log file name | status) elements
     */
    String[] getRedoLogFileState();

    /**
     * fetches history of redo switches for the last hour
     * @return array of: (redo log file name | time of switch) elements
     */
    String[] getSwitchHistory();

    /**
     * todo Java doc
     * @return
     */
    String[] getSequences();

    int getTodaySwitchCount();

    Long getLastFetchingQueryDuration();

    int getNumberOfFetchedDml();

    int getExecutedFetchingQueriesCount();

    Long getAverageFetchingQueryDuration();

    Long getDurationOfLastProcessedBatch();

    int getProcessedBatchCount();

    Long getAverageProcessedBatchDuration();

    int getMaxMiningBatchSize();

    void  setMaxMiningBatchSize(int size);

    int getMillisecondsToSleepBetweenMiningQuery();

    void setMillisecondsToSleepBetweenMiningQuery(int milliseconds);

    int getFetchedRecordsSizeLimitToFallAsleep();

    void setFetchedRecordsSizeLimitToFallAsleep(int size);
}
