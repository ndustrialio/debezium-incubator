/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class contains methods to be exposed via MBean server
 *
 */
@ThreadSafe
public class LogMinerMetrics extends Metrics implements LogMinerMetricsMXBean {
    private AtomicLong currentScn = new AtomicLong();
    private AtomicInteger capturedDmlCount = new AtomicInteger();
    private AtomicReference<String> currentLogFileName;
    private AtomicReference<String[]> redoLogStatuses;
    private AtomicReference<String[]> switchHistory;
    private AtomicReference<String[]> redoLogFileStatuses;
    private AtomicReference<String[]> redoLogSequences;
    private AtomicReference<Duration> lastFetchingQueryDuration;
    private AtomicReference<Duration> averageFetchingQueryDuration = new AtomicReference<>();
    private AtomicInteger fetchingQueryCount = new AtomicInteger();
    private AtomicReference<Duration> lastProcessedCapturedBatchDuration;
    private AtomicInteger processedCapturedBatchCount = new AtomicInteger();
    private AtomicReference<Duration> averageProcessedCapturedBatchDuration = new AtomicReference<>();
    private AtomicInteger maxMiningBatchSize = new AtomicInteger();
    private AtomicInteger millisecondsToSleepBetweenMiningQuery = new AtomicInteger();
    private AtomicInteger fetchedRecordsSizeLimitToFallAsleep = new AtomicInteger();

    LogMinerMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner");

        maxMiningBatchSize.set(1000);
        millisecondsToSleepBetweenMiningQuery.set(500);
        fetchedRecordsSizeLimitToFallAsleep.set(50);

        currentScn.set(-1);
        capturedDmlCount.set(-1);
        currentLogFileName = new AtomicReference<>();
        redoLogFileStatuses = new AtomicReference<>();
        redoLogStatuses = new AtomicReference<>();
        redoLogSequences = new AtomicReference<>();
        switchHistory = new AtomicReference<>();
        lastFetchingQueryDuration = new AtomicReference<>();
        fetchingQueryCount.set(-1);
        lastProcessedCapturedBatchDuration = new AtomicReference<>();
        processedCapturedBatchCount.set(-1);
    }

    // setters
    public void setCurrentScn(Long scn){
        currentScn.set(scn);
    }

    public void setCapturedDmlCount(int count){
        capturedDmlCount.set(count);
    }

    public void setCurrentLogFileName(String name){
        currentLogFileName.set(name);
    }

    public void setRedoLogState(Map<String, String> statuses){
        String[] statusArray = statuses.entrySet().stream().map(e -> e.getKey() + " | " + e.getValue()).toArray(String[]::new);
        redoLogStatuses.set(statusArray);
    }

    public void setSwitchHistory(Map<String, String> history) {
        String[] historyArray = history.entrySet().stream().map(e -> e.getKey() + " | " + e.getValue()).toArray(String[]::new);
        switchHistory.set(historyArray);
    }

    public void setRedoLogFileStatuses(Map<String, String> statuses) {
        String[] statusArray = statuses.entrySet().stream().map(e -> e.getKey() + " | " + e.getValue()).toArray(String[]::new);
        redoLogFileStatuses.set(statusArray);
    }

    public void setRedoLogSequences(Map<String, String> sequences){
        String[] sequenceArray = sequences.entrySet().stream().map(e -> e.getKey() + " | " + e.getValue()).toArray(String[]::new);
        redoLogSequences.set(sequenceArray);
    }

    public void setLastFetchingQueryDuration(Duration fetchDuration){
        setDurationMetrics(fetchDuration, lastFetchingQueryDuration, fetchingQueryCount, averageFetchingQueryDuration);
    }

    public void setProcessedCapturedBatchDuration(Duration processDuration){
        setDurationMetrics(processDuration, lastProcessedCapturedBatchDuration, processedCapturedBatchCount, averageProcessedCapturedBatchDuration);
    }

    // implemented getters
    @Override
    public Long getCurrentScn() {
        return currentScn.get();
    }

    @Override
    public int getNumberOfFetchedDml() {
        return capturedDmlCount.get();
    }

    @Override
    public String getCurrentRedoLogFile() {
        return currentLogFileName.get();
    }

    @Override
    public String[] getRedoLogStatuses() {
        return redoLogStatuses.get();
    }

    @Override
    public String[] getRedoLogFileState() {
        return redoLogFileStatuses.get();
    }

    @Override
    public String[] getSwitchHistory() {
        return switchHistory.get();
    }

    @Override
    public String[] getSequences() {
        return redoLogSequences.get();
    }

    @Override
    public int getTodaySwitchCount() {
        return switchHistory.get().length;
    }

    @Override
    public Long getLastFetchingQueryDuration() {
        return lastFetchingQueryDuration.get() == null ? 0 : lastFetchingQueryDuration.get().toMillis();
    }

    @Override
    public Long getAverageFetchingQueryDuration() {
        return averageFetchingQueryDuration.get() == null ? 0 : averageFetchingQueryDuration.get().toMillis();
    }

    @Override
    public Long getDurationOfLastProcessedBatch() {
        return lastProcessedCapturedBatchDuration.get() == null ? 0 : lastProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getExecutedFetchingQueriesCount() {
        return fetchingQueryCount.get();
    }

    @Override
    public int getProcessedBatchCount() {
        return processedCapturedBatchCount.get();
    }

    @Override
    public Long getAverageProcessedBatchDuration() {
        return averageProcessedCapturedBatchDuration.get() == null ? 0 : averageProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getMaxMiningBatchSize() {
        return maxMiningBatchSize.get();
    }

    @Override
    public int getMillisecondsToSleepBetweenMiningQuery() {
        return millisecondsToSleepBetweenMiningQuery.get();
    }

    @Override
    public int getFetchedRecordsSizeLimitToFallAsleep() {
        return fetchedRecordsSizeLimitToFallAsleep.get();
    }

    // MBean accessible setters
    @Override
    public void setMaxMiningBatchSize(int size) {
        if (size >= 100 && size <= 10_000) {
            maxMiningBatchSize.set(size);
        }
    }

    @Override
    public void setMillisecondsToSleepBetweenMiningQuery(int milliseconds) {
        if (milliseconds > 0 && milliseconds < 3000){
            millisecondsToSleepBetweenMiningQuery.set(milliseconds);
        }

    }

    @Override
    public void setFetchedRecordsSizeLimitToFallAsleep(int size) {
        if (size > 0 && size < 500) {
            fetchedRecordsSizeLimitToFallAsleep.set(size);
        }
    }

    // private methods
    private void setDurationMetrics(Duration duration, AtomicReference<Duration> lastDuration, AtomicInteger counter,
                                    AtomicReference<Duration> averageDuration){
        if (duration != null) {
            lastDuration.set(duration);
            int count = counter.incrementAndGet();
            Duration currentAverage = averageDuration.get() == null ? Duration.ZERO : averageDuration.get();
            averageDuration.set(currentAverage.multipliedBy(count - 1).plus(duration).dividedBy(count));
        }
    }

    @Override
    public String toString() {
        return "LogMinerMetrics{" +
                "currentScn=" + currentScn.get() +
                ", currentLogFileName=" + currentLogFileName.get() +
                ", redoLogStatuses=" + Arrays.toString(redoLogStatuses.get()) +
                ", capturedDmlCount=" + capturedDmlCount.get() +
                ", switchHistory=" + Arrays.toString(switchHistory.get()) +
                ", redoLogFileStatuses=" + Arrays.toString(redoLogFileStatuses.get()) +
                ", redoLogSequences=" + Arrays.toString(redoLogSequences.get()) +
                ", lastFetchingQueryDuration=" + lastFetchingQueryDuration.get().toMillis() +
                ", fetchingQueryCount=" + fetchingQueryCount.get() +
                ", averageFetchingQueryDuration=" + averageFetchingQueryDuration.get().toMillis() +
                ", lastProcessedCapturedBatchDuration=" + lastProcessedCapturedBatchDuration.get().toMillis() +
                ", processedCapturedBatchCount=" + processedCapturedBatchCount.get() +
                ", averageProcessedCapturedBatchDuration=" + averageProcessedCapturedBatchDuration.get().toMillis() +
                ", maxMiningBatchSize=" + maxMiningBatchSize.get() +
                ", millisecondsToSleepBetweenMiningQuery=" + millisecondsToSleepBetweenMiningQuery.get() +
                ", maxMiningBatchSize=" + maxMiningBatchSize.get() +
                '}';
    }
}
