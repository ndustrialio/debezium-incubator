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
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class contains MBean methods
 */
@ThreadSafe
public class TransactionalBufferMetrics extends Metrics implements TransactionalBufferMetricsMXBean {
    private AtomicLong oldestScn = new AtomicLong();
    private AtomicLong committedScn = new AtomicLong();
    private AtomicReference<Duration> lagFromTheSource = new AtomicReference<>();
    private AtomicInteger activeTransactions = new AtomicInteger();
    private AtomicLong rolledBackTransactions = new AtomicLong();
    private AtomicLong committedTransactions = new AtomicLong();
    private AtomicLong capturedDmlCounter = new AtomicLong();
    private AtomicLong committedDmlCounter = new AtomicLong();
    private AtomicReference<Duration> maxLagFromTheSource = new AtomicReference<>();
    private AtomicReference<Duration> minLagFromTheSource = new AtomicReference<>();
    private AtomicReference<Duration> averageLagsFromTheSource = new AtomicReference<>();
    private AtomicReference<Set<String>> abandonedTransactionIds = new AtomicReference<>();
    private AtomicReference<Set<String>> rolledBackTransactionIds = new AtomicReference<>();
    private Instant startTime;
    private static long MILLIS_PER_SECOND = 1000L;
    private AtomicLong timeDifference = new AtomicLong();
    private AtomicInteger errorCounter = new AtomicInteger();
    private AtomicInteger warningCounter = new AtomicInteger();
    private AtomicInteger scnFreezeCounter = new AtomicInteger();



    // temp todo delete after stowplan testing
    private Long ufvDelete = 0L;
    private Long ufvInsert = 0L;
    private Long wiDelete = 0L;
    private Long wiInsert = 0L;
    private Long rtdDelete = 0L;
    private Long rtdInsert = 0L;

    public Long getUfvDelete() {
        return ufvDelete;
    }

    public void incrementUfvDelete() {
        this.ufvDelete++;
    }

    public Long getUfvInsert() {
        return ufvInsert;
    }

    public void incrementUfvInsert() {
        this.ufvInsert++;
    }
    @Override
    public Long getWiDelete() {
        return wiDelete;
    }

    @Override
    public void incrementWiDelete() {
        this.wiDelete++;
    }

    @Override
    public Long getWiInsert() {
        return wiInsert;
    }

    @Override
    public void incrementWiInsert() {
        wiInsert++;
    }

    @Override
    public Long getRTDDelete() {
        return rtdDelete;
    }

    @Override
    public void incrementRTDDelete() {
        rtdDelete++;
    }

    @Override
    public Long getRTDInsert() {
        return rtdInsert;
    }

    @Override
    public void incrementRTDInsert() {
        rtdInsert++;
    }

    TransactionalBufferMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner-transactional-buffer");
        startTime = Instant.now();
        oldestScn.set(-1);
        committedScn.set(-1);
        timeDifference.set(0);
        reset();
    }

    // setters
    void setOldestScn(Long scn){
        oldestScn.set(scn);
    }

    public void setCommittedScn(Long scn){
        committedScn.set(scn);
    }

    public void setTimeDifference(AtomicLong timeDifference) {
        this.timeDifference = timeDifference;
    }

    void calculateLagMetrics(Instant changeTime){
        if (changeTime != null) {
            Instant correctedChangeTime = changeTime.plus(Duration.ofMillis(timeDifference.longValue()));
            lagFromTheSource.set(Duration.between(correctedChangeTime, Instant.now()));

            if (maxLagFromTheSource.get().toMillis() < lagFromTheSource.get().toMillis()) {
                maxLagFromTheSource.set(lagFromTheSource.get());
            }
            if (minLagFromTheSource.get().toMillis() > lagFromTheSource.get().toMillis()) {
                minLagFromTheSource.set(lagFromTheSource.get());
            }

            if (averageLagsFromTheSource.get().isZero()) {
                averageLagsFromTheSource.set(lagFromTheSource.get());
            } else {
                averageLagsFromTheSource.set(averageLagsFromTheSource.get().plus(lagFromTheSource.get()).dividedBy(2));
            }
        }
    }

    void setActiveTransactions(Integer counter){
        if (counter != null) {
            activeTransactions.set(counter);
        }
    }

    void incrementRolledBackTransactions(){
        rolledBackTransactions.incrementAndGet();
    }

    void incrementCommittedTransactions(){
        committedTransactions.incrementAndGet();
    }

    void incrementCapturedDmlCounter() {
        capturedDmlCounter.incrementAndGet();
    }

    void incrementCommittedDmlCounter(int counter) {
        committedDmlCounter.getAndAdd(counter);
    }

    void addAbandonedTransactionId(String transactionId){
        if (transactionId != null) {
            abandonedTransactionIds.get().add(transactionId);
        }
    }

    void addRolledBackTransactionId(String transactionId){
        if (transactionId != null) {
            rolledBackTransactionIds.get().add(transactionId);
        }
    }

    /**
     * This is to increase logged logError counter.
     * There are other ways to monitor the log, but this is just to check if there are any.
     */
    void incrementErrorCounter() {
        errorCounter.incrementAndGet();
    }

    /**
     * This is to increase logged warning counter
     * There are other ways to monitor the log, but this is just to check if there are any.
     */
    void incrementWarningCounter() {
        warningCounter.incrementAndGet();
    }

    /**
     * This counter to accumulate number of encountered observations when SCN does not change in the offset.
     * This call indicates an uncommitted oldest transaction in the buffer.
     */
    void incrementScnFreezeCounter() {
        scnFreezeCounter.incrementAndGet();
    }

    // implemented getters
    @Override
    public Long getOldestScn() {
        return oldestScn.get();
    }

    @Override
    public Long getCommittedScn() {
        return committedScn.get();
    }

    @Override
    public int getNumberOfActiveTransactions() {
        return activeTransactions.get();
    }

    @Override
    public long getNumberOfRolledBackTransactions() {
        return rolledBackTransactions.get();
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return committedTransactions.get();
    }

    @Override
    public long getCommitThroughput() {
        return committedTransactions.get() * MILLIS_PER_SECOND / Duration.between(startTime, Instant.now()).toMillis();
    }

    @Override
    public long getCapturedDmlThroughput() {
        return committedDmlCounter.get() * MILLIS_PER_SECOND / Duration.between(startTime, Instant.now()).toMillis();
    }

    @Override
    public long getCapturedDmlCount() {
        return capturedDmlCounter.longValue();
    }

    @Override
    public long getLagFromSource() {
        return lagFromTheSource.get().toMillis();
    }

    @Override
    public long getMaxLagFromSource() {
        return maxLagFromTheSource.get().toMillis();
    }

    @Override
    public long getMinLagFromSource() {
        return minLagFromTheSource.get().toMillis();
    }

    @Override
    public long getAverageLagFromSource() {
        return averageLagsFromTheSource.get().toMillis();
    }

    @Override
    public Set<String> getAbandonedTransactionIds() {
        return abandonedTransactionIds.get();
    }

    @Override
    public Set<String> getRolledBackTransactionIds() {
        return rolledBackTransactionIds.get();
    }

    @Override
    public int getErrorCounter() {
        return errorCounter.get();
    }

    @Override
    public int getWarningCounter() {
        return warningCounter.get();
    }

    @Override
    public int getScnFreezeCounter() {
        return scnFreezeCounter.get();
    }

    @Override
    public void reset() {
        maxLagFromTheSource.set(Duration.ZERO);
        minLagFromTheSource.set(Duration.ZERO);
        averageLagsFromTheSource.set(Duration.ZERO);
        activeTransactions.set(0);
        rolledBackTransactions.set(0);
        committedTransactions.set(0);
        capturedDmlCounter.set(0);
        committedDmlCounter.set(0);
        abandonedTransactionIds.set(new HashSet<>());
        rolledBackTransactionIds.set(new HashSet<>());
        lagFromTheSource.set(Duration.ZERO);
        errorCounter.set(0);
        warningCounter.set(0);
        scnFreezeCounter.set(0);
        ufvDelete = 0L;
        ufvInsert = 0L;
        wiInsert = 0L;
        wiDelete = 0L;
        rtdInsert = 0L;
        rtdDelete = 0L;
    }

    @Override
    public String toString() {
        return "TransactionalBufferMetrics{" +
                "oldestScn=" + oldestScn.get() +
                ", committedScn=" + committedScn.get() +
                ", lagFromTheSource=" + lagFromTheSource.get() +
                ", activeTransactions=" + activeTransactions.get() +
                ", rolledBackTransactions=" + rolledBackTransactions.get() +
                ", committedTransactions=" + committedTransactions.get() +
                ", capturedDmlCounter=" + capturedDmlCounter.get() +
                ", committedDmlCounter=" + committedDmlCounter.get() +
                ", maxLagFromTheSource=" + maxLagFromTheSource.get() +
                ", minLagFromTheSource=" + minLagFromTheSource.get() +
                ", averageLagsFromTheSource=" + averageLagsFromTheSource.get() +
                ", abandonedTransactionIds=" + abandonedTransactionIds.get() +
                ", errorCounter=" + errorCounter.get() +
                ", warningCounter=" + warningCounter.get() +
                ", scnFreezeCounter=" + scnFreezeCounter.get() +
                '}';
    }
}
