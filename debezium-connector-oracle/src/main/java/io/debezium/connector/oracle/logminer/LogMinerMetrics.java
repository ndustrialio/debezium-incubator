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
    private AtomicReference<String> currentLogFileName = new AtomicReference<>();
    private AtomicReference<String[]> redoLogStatuses = new AtomicReference<>();
    private AtomicReference<String[]> switchHistory = new AtomicReference<>();
    private AtomicReference<String[]> redoLogFileStatuses = new AtomicReference<>();
    private AtomicReference<String[]> redoLogSequences = new AtomicReference<>();
    private AtomicReference<Duration> lastFetchingQueryDuration = new AtomicReference<>();
    private AtomicReference<Duration> averageFetchingQueryDuration = new AtomicReference<>();
    private AtomicInteger fetchingQueryCount = new AtomicInteger();
    private AtomicReference<Duration> lastProcessedCapturedBatchDuration = new AtomicReference<>();
    private AtomicInteger processedCapturedBatchCount = new AtomicInteger();
    private AtomicReference<Duration> averageProcessedCapturedBatchDuration = new AtomicReference<>();
    private AtomicInteger maxMiningBatchSize = new AtomicInteger();

    LogMinerMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner");
    }

    public void setCurrentScn(Long scn){
        currentScn.set(scn);
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
        setMetrics(fetchDuration, lastFetchingQueryDuration, fetchingQueryCount, averageFetchingQueryDuration);
    }

    public void setProcessedCapturedBatchDuration(Duration processDuration){
        setMetrics(processDuration, lastProcessedCapturedBatchDuration, processedCapturedBatchCount, averageProcessedCapturedBatchDuration);
    }

    private void setMetrics(Duration duration, AtomicReference<Duration> lastDuration, AtomicInteger counter,
                            AtomicReference<Duration> averageDuration){
        if (duration != null) {
            lastDuration.set(duration);
            int count = counter.incrementAndGet();
            Duration currentAverage = averageDuration.get() == null ? Duration.ZERO : averageDuration.get();
            averageDuration.set(currentAverage.multipliedBy(count - 1).plus(duration).dividedBy(count));
        }
    }

    public void setCapturedDmlCount(int count){
        capturedDmlCount.set(count);
    }

    @Override
    public int getNumberOfCapturedDml() {
        return capturedDmlCount.get();
    }

    @Override
    public int getLastDaySwitchNumber() {
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
    public Long getLastProcessedCapturedBatchDuration() {
        return lastProcessedCapturedBatchDuration.get() == null ? 0 : lastProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getExecutedFetchingQueriesCount() {
        return fetchingQueryCount.get();
    }

    @Override
    public int getProcessesCapturedBatchesCount() {
        return processedCapturedBatchCount.get();
    }

    @Override
    public Long getAverageProcessedCapturedBatchDuration() {
        return averageProcessedCapturedBatchDuration.get() == null ? 0 : averageProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getMaxMiningBatchSize() {
        return maxMiningBatchSize.get() == 0 ? 1000 : maxMiningBatchSize.get();
    }

    @Override
    public void setMaxMiningBatchSize(int size) {
        if (size >= 100 && size <= 10_000) {
            maxMiningBatchSize.set(size);
        }
    }

    @Override
    public Long getCurrentScn() {
        return currentScn.get();
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

    void reset(){
        redoLogFileStatuses = new AtomicReference<>();
        redoLogStatuses = new AtomicReference<>();
        redoLogSequences = new AtomicReference<>();
        switchHistory = new AtomicReference<>();
        currentScn.set(-1);
    }

    @Override
    public String toString() {
        return "LogMinerMetrics{" +
                "currentScn=" + currentScn +
                ", currentLogFileName=" + currentLogFileName +
                ", redoLogStatuses=" + redoLogStatuses +
                ", capturedDmlCount=" + capturedDmlCount +
                ", switchHistory=" + Arrays.toString(switchHistory.get()) +
                ", redoLogFileStatuses=" + Arrays.toString(redoLogFileStatuses.get()) +
                ", redoLogSequences=" + Arrays.toString(redoLogSequences.get()) +
                ", lastFetchingQueryDuration=" + lastFetchingQueryDuration +
                ", fetchingQueryCount=" + fetchingQueryCount +
                ", averageFetchingQueryDuration=" + averageFetchingQueryDuration +
                ", lastProcessedCapturedBatchDuration=" + lastProcessedCapturedBatchDuration +
                ", processedCapturedBatchCount=" + processedCapturedBatchCount +
                ", averageProcessedCapturedBatchDuration=" + averageProcessedCapturedBatchDuration +
                ", maxMiningBatchSize=" + maxMiningBatchSize +
                '}';
    }
}
