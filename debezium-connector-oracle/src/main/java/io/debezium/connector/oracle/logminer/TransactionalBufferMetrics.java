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
    private AtomicReference<Duration> lagFromTheSource = new AtomicReference<>();
    private AtomicInteger activeTransactions = new AtomicInteger();
    private AtomicLong rolledBackTransactions = new AtomicLong();
    private AtomicLong committedTransactions = new AtomicLong();
    private AtomicLong capturedDmlCounter = new AtomicLong();
    private AtomicLong committedDmlCounter = new AtomicLong();
    private AtomicReference<Duration> maxLagFromTheSource = new AtomicReference<>();
    private AtomicReference<Duration> minLagFromTheSource = new AtomicReference<>();
    private AtomicReference<Duration> totalLagsFromTheSource = new AtomicReference<>();
    private AtomicReference<Set<String>> abandonedTransactionIds = new AtomicReference<>();
    private Instant startTime;
    private static long MILLIS_PER_SECOND = 1000L;

    TransactionalBufferMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner-transactional-buffer");
        startTime = Instant.now();
        oldestScn.set(-1);
        lagFromTheSource.set(Duration.ZERO);
        maxLagFromTheSource.set(Duration.ZERO);
        minLagFromTheSource.set(Duration.ZERO);
        totalLagsFromTheSource.set(Duration.ZERO);
        activeTransactions.set(0);
        rolledBackTransactions.set(0);
        committedTransactions.set(0);
        capturedDmlCounter.set(0);
        committedDmlCounter.set(0);
        totalLagsFromTheSource.set(Duration.ZERO);
        abandonedTransactionIds.set(new HashSet<>());
    }

    // setters
    void setOldestScn(Long scn){
        oldestScn.set(scn);
    }

    void setLagFromTheSource(Instant changeTime){
        if (changeTime != null) {
            lagFromTheSource.set(Duration.between(changeTime, Instant.now()));
            if (maxLagFromTheSource.get().toMillis() < lagFromTheSource.get().toMillis()) {
                maxLagFromTheSource.set(lagFromTheSource.get());
            }
            if (minLagFromTheSource.get().toMillis() > lagFromTheSource.get().toMillis()) {
                minLagFromTheSource.set(lagFromTheSource.get());
            }
            totalLagsFromTheSource.set(totalLagsFromTheSource.get().plus(lagFromTheSource.get()));
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

    void decrementCapturedDmlCounter(int counter) {
        capturedDmlCounter.getAndAdd(-counter);
    }

    void incrementCommittedDmlCounter(int counter) {
        committedDmlCounter.getAndAdd(counter);
    }

    void addAbandonedTransactionId(String transactionId){
        if (transactionId != null) {
            abandonedTransactionIds.get().add(transactionId);
        }
    }

    // implemented getters
    @Override
    public Long getOldestScn() {
        return oldestScn.get();
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
        return totalLagsFromTheSource.get().toMillis()/capturedDmlCounter.get() == 0 ? 1 : capturedDmlCounter.get();
    }

    @Override
    public Set<String> getAbandonedTransactionIds() {
        return abandonedTransactionIds.get();
    }

    @Override
    public String toString() {
        return "TransactionalBufferMetrics{" +
                "oldestScn=" + oldestScn.get() +
                ", lagFromTheSource=" + lagFromTheSource.get() +
                ", activeTransactions=" + activeTransactions.get() +
                ", rolledBackTransactions=" + rolledBackTransactions.get() +
                ", committedTransactions=" + committedTransactions.get() +
                ", capturedDmlCounter=" + capturedDmlCounter.get() +
                ", committedDmlCounter=" + committedDmlCounter.get() +
                ", maxLagFromTheSource=" + maxLagFromTheSource.get() +
                ", minLagFromTheSource=" + minLagFromTheSource.get() +
                ", totalLagsFromTheSource=" + totalLagsFromTheSource.get() +
                ", abandonedTransactionIds=" + abandonedTransactionIds.get() +
                '}';
    }
}
