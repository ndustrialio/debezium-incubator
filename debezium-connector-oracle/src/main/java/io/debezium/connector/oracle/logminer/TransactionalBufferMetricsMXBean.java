/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

/**
 * This interface exposes TransactionalBuffer metrics
 */
public interface TransactionalBufferMetricsMXBean {

    /**
     * Exposes number of rolled back transactions
     *
     * @return number of rolled back transaction in the in-memory buffer
     */
    long getNumberOfRolledBackTransactions();

    /**
     * Exposes number of committed transactions
     *
     * @return number of committed transaction in the in-memory buffer
     */
    long getNumberOfCommittedTransactions();

    /**
     * Exposes average number of committed transactions per second
     *
     * @return average number of committed transactions per second in the in-memory buffer
     */
    long getCommitThroughput();

    /**
     * Exposes average number of captured and parsed DML per second
     *
     * @return average number of captured and parsed DML per second in the in-memory buffer
     */
    long getDmlThroughput();

    /**
     * Exposes number of transaction, buffered in memory
     *
     * @return number of currently buffered transactions
     */
    int getNumberOfActiveTransactions();

    /**
     * Exposes the oldest(smallest) in the in-memory buffer SCN
     *
     * @return oldest SCN
     */
    Long getOldestScn();

    /**
     * This is to get the lag between latest captured change timestamp and time of it's placement in the buffer
     * @return lag in milliseconds
     */
    long getLagFromSource();

}
