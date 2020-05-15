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
import java.util.Set;
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
    private AtomicReference<String[]> currentLogFileName;
    private AtomicReference<String[]> redoLogStatus;
    private AtomicInteger switchCounter = new AtomicInteger();
    private AtomicReference<Duration> lastLogMinerQueryDuration = new AtomicReference<>();
    private AtomicReference<Duration> averageLogMinerQueryDuration = new AtomicReference<>();
    private AtomicInteger logMinerQueryCount = new AtomicInteger();
    private AtomicReference<Duration> lastProcessedCapturedBatchDuration = new AtomicReference<>();
    private AtomicInteger processedCapturedBatchCount = new AtomicInteger();
    private AtomicReference<Duration> averageProcessedCapturedBatchDuration = new AtomicReference<>();
    private AtomicInteger maxBatchSize = new AtomicInteger();
    private AtomicInteger millisecondToSleepBetweenMiningQuery = new AtomicInteger();
    private AtomicInteger fetchedRecordSizeToSleepMore = new AtomicInteger();

    private final int MAX_SLEEP_TIME = 3_000;
    private final int DEFAULT_SLEEP_TIME = 1_000;
    private final int MIN_SLEEP_TIME = 100;

    private final int MIN_BATCH_SIZE = 100;
    private final int MAX_BATCH_SIZE = 100_000;
    private final int DEFAULT_BATCH_SIZE = 10_000;

    private final int SLEEP_TIME_INCREMENT = 200;
    private final int SIZE_TO_SLEEP_LONGER = 50;

    LogMinerMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner");

        maxBatchSize.set(DEFAULT_BATCH_SIZE);
        millisecondToSleepBetweenMiningQuery.set(DEFAULT_SLEEP_TIME);
        fetchedRecordSizeToSleepMore.set(SIZE_TO_SLEEP_LONGER);

        currentScn.set(-1);
        capturedDmlCount.set(0);
        currentLogFileName = new AtomicReference<>();
        redoLogStatus = new AtomicReference<>();
        switchCounter.set(0);
        averageLogMinerQueryDuration.set(Duration.ZERO);
        lastLogMinerQueryDuration.set(Duration.ZERO);
        logMinerQueryCount.set(0);
        lastProcessedCapturedBatchDuration.set(Duration.ZERO);
        processedCapturedBatchCount.set(0);
        averageProcessedCapturedBatchDuration.set(Duration.ZERO);
    }

    // setters
    public void setCurrentScn(Long scn){
        currentScn.set(scn);
    }

    public void setCapturedDmlCount(int count){
        capturedDmlCount.set(count);
    }

    public void setCurrentLogFileName(Set<String> names){
        currentLogFileName.set(names.stream().toArray(String[]::new));
    }

    public void setRedoLogStatus(Map<String, String> status){
        String[] statusArray = status.entrySet().stream().map(e -> e.getKey() + " | " + e.getValue()).toArray(String[]::new);
        redoLogStatus.set(statusArray);
    }

    public void setSwitchCount(int counter) {
        switchCounter.set(counter);
    }

    public void setLastLogMinerQueryDuration(Duration fetchDuration){
        setDurationMetrics(fetchDuration, lastLogMinerQueryDuration, logMinerQueryCount, averageLogMinerQueryDuration);
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
    public int getCapturedDmlCount() {
        return capturedDmlCount.get();
    }

    @Override
    public String[] getCurrentRedoLogFileName() {
        return currentLogFileName.get();
    }

    @Override
    public String[] getRedoLogStatus() {
        return redoLogStatus.get();
    }

    @Override
    public int getSwitchCounter() {
        return switchCounter.get();
    }

    @Override
    public Long getLastLogMinerQueryDuration() {
        return lastLogMinerQueryDuration.get() == null ? 0 : lastLogMinerQueryDuration.get().toMillis();
    }

    @Override
    public Long getAverageLogMinerQueryDuration() {
        return averageLogMinerQueryDuration.get() == null ? 0 : averageLogMinerQueryDuration.get().toMillis();
    }

    @Override
    public Long getLastProcessedCapturedBatchDuration() {
        return lastProcessedCapturedBatchDuration.get() == null ? 0 : lastProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getLogMinerQueryCount() {
        return logMinerQueryCount.get();
    }

    @Override
    public int getProcessedCapturedBatchCount() {
        return processedCapturedBatchCount.get();
    }

    @Override
    public Long getAverageProcessedCapturedBatchDuration() {
        return averageProcessedCapturedBatchDuration.get() == null ? 0 : averageProcessedCapturedBatchDuration.get().toMillis();
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize.get();
    }

    @Override
    public Integer getMillisecondToSleepBetweenMiningQuery() {
        return millisecondToSleepBetweenMiningQuery.get();
    }

    @Override
    public int getFetchedRecordSizeToSleepMore() {
        return fetchedRecordSizeToSleepMore.get();
    }

    // MBean accessible setters
    @Override
    public void setMaxBatchSize(int size) {
        if (size >= MIN_BATCH_SIZE && size <= MAX_BATCH_SIZE) {
            maxBatchSize.set(size);
        }
    }

    @Override
    public void setMillisecondToSleepBetweenMiningQuery(Integer milliseconds) {
        if (milliseconds != null && milliseconds >= MIN_SLEEP_TIME && milliseconds < MAX_SLEEP_TIME){
            millisecondToSleepBetweenMiningQuery.set(milliseconds);
        }
    }

    @Override
    public void incrementSleepingTime() {
        int sleepTime = millisecondToSleepBetweenMiningQuery.get();
        if (sleepTime >= MIN_SLEEP_TIME && sleepTime < MAX_SLEEP_TIME){
            millisecondToSleepBetweenMiningQuery.getAndAdd(SLEEP_TIME_INCREMENT);
        }
    }

    @Override
    public void resetSleepingTime() {
        int sleepTime = millisecondToSleepBetweenMiningQuery.get();
        if (sleepTime >= MIN_SLEEP_TIME && sleepTime < MAX_SLEEP_TIME){
            millisecondToSleepBetweenMiningQuery.set(MIN_SLEEP_TIME);
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
                "currentEndScn=" + currentScn.get() +
                ", currentLogFileNames=" + Arrays.toString(currentLogFileName.get()) +
                ", redoLogStatus=" + Arrays.toString(redoLogStatus.get()) +
                ", capturedDmlCount=" + capturedDmlCount.get() +
                ", switchCounter=" + switchCounter.get() +
                ", lastLogMinerQueryDuration=" + lastLogMinerQueryDuration.get() +
                ", logMinerQueryCount=" + logMinerQueryCount.get() +
                ", averageLogMinerQueryDuration=" + averageLogMinerQueryDuration.get() +
                ", lastProcessedCapturedBatchDuration=" + lastProcessedCapturedBatchDuration.get() +
                ", processedCapturedBatchCount=" + processedCapturedBatchCount.get() +
                ", averageProcessedCapturedBatchDuration=" + averageProcessedCapturedBatchDuration.get() +
                ", maxBatchSize=" + maxBatchSize.get() +
                ", millisecondToSleepBetweenMiningQuery=" + millisecondToSleepBetweenMiningQuery.get() +
                ", maxBatchSize=" + maxBatchSize.get() +
                '}';
    }
}
