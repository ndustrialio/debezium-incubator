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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class contains methods to be MBean exposed
 */
@ThreadSafe
public class TransactionalBufferMetrics extends Metrics implements TransactionalBufferMetricsMXBean {
    private AtomicLong smallestScn = new AtomicLong();
    private AtomicReference<Duration> lagFromTheSource = new AtomicReference<>();
    private AtomicInteger activeTransactions = new AtomicInteger();
    private AtomicLong rolledBackTransactions = new AtomicLong();


    TransactionalBufferMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "log-miner-transactional-buffer");
    }

    void setSmallestScn(Long scn){
        smallestScn.set(scn);
    }

    void setLagFromTheSource(Instant changeTime){
        if (changeTime != null) {
            lagFromTheSource.set(Duration.between(changeTime, Instant.now()));
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

    void reset(){
        smallestScn.set(-1);
        lagFromTheSource.set(null);
        activeTransactions.set(0);
        rolledBackTransactions.set(0);
    }

    @Override
    public Long getOldestScn() {
        return smallestScn.get();
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
    public long getLagFromSource() {
        Duration lag =  lagFromTheSource.get();
        return lag != null ? lag.toMillis() : -1;
    }

    @Override
    public String toString() {
        return "TransactionalBufferMetrics{" +
                "smallestScn=" + smallestScn +
                ", lagFromTheSource=" + lagFromTheSource.get() +
                ", activeTransactions=" + activeTransactions +
                ", rolledBackTransactions=" + rolledBackTransactions +
                '}';
    }
}
