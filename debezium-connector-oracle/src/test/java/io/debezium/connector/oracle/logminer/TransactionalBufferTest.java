/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Andrey Pustovetov
 */

public class TransactionalBufferTest {

    private static final String SERVER_NAME = "serverX";
    private static final String TRANSACTION_ID = "transaction";
    private static final String OTHER_TRANSACTION_ID = "other_transaction";
    private static final String SQL_ONE = "update table";
    private static final String SQL_TWO = "insert into table";
    private static final String MESSAGE = "OK";
    private static final BigDecimal SCN = BigDecimal.ONE;
    private static final BigDecimal OTHER_SCN = BigDecimal.TEN;
    private static final BigDecimal LARGEST_SCN = BigDecimal.valueOf(100L);
    private static final Timestamp TIMESTAMP = new Timestamp(System.currentTimeMillis());

    private ErrorHandler errorHandler;
    private TransactionalBuffer transactionalBuffer;

    @Before
    public void before() {
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .build();
        errorHandler = new ErrorHandler(OracleConnector.class, SERVER_NAME, queue, () -> { });
        transactionalBuffer = new TransactionalBuffer(SERVER_NAME, errorHandler, null);
    }

    @After
    public void after() throws InterruptedException {
        errorHandler.stop();
        transactionalBuffer.close();
    }

    @Test
    public void testIsEmpty() {
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testIsNotEmptyWhenTransactionIsRegistered() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> { });
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
    }

    @Test
    public void testIsNotEmptyWhenTransactionIsCommitting() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> Thread.sleep(1000));
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true, MESSAGE);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
    }

    @Test
    public void testIsEmptyWhenTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> commitLatch.countDown());
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true, MESSAGE);
        commitLatch.await();
        Thread.sleep(1000);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testIsEmptyWhenTransactionIsRolledBack() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> { });
        transactionalBuffer.rollback(TRANSACTION_ID, "");
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testCalculateSmallestScnWhenTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<BigDecimal> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true, MESSAGE);
        commitLatch.await();
        assertThat(smallestScnContainer.get()).isEqualTo(SCN);
    }

    @Test
    public void testCalculateSmallestScnWhenFirstTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<BigDecimal> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), "", (timestamp, smallestScn) -> { });
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true, MESSAGE);
        commitLatch.await();
        assertThat(smallestScnContainer.get()).isEqualTo(SCN);
    }

    @Test
    public void testCalculateSmallestScnWhenSecondTransactionIsCommitted() throws InterruptedException {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> { });
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<BigDecimal> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), "", (timestamp, smallestScn) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        transactionalBuffer.commit(OTHER_TRANSACTION_ID, TIMESTAMP, () -> true, MESSAGE);
        commitLatch.await();
        assertThat(smallestScnContainer.get()).isEqualTo(SCN);
    }

    @Test
    public void testAbandoningTransaction() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> {});
        transactionalBuffer.abandonLongTransactions(SCN.longValue());
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);

        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), "", (timestamp, smallestScn) -> {});
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), "", (timestamp, smallestScn) -> {});
        transactionalBuffer.abandonLongTransactions(SCN.longValue());
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
    }

    @Test
    public void testTransactionDump() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), SQL_ONE, (timestamp, smallestScn) -> {});
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), SQL_ONE, (timestamp, smallestScn) -> {});
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), SQL_TWO, (timestamp, smallestScn) -> {});
        assertThat(transactionalBuffer.toString()).contains(SQL_ONE);
        assertThat(transactionalBuffer.toString()).contains(SQL_TWO);
    }

    @Test
    public void testDuplicatedRedoSql() {

        assertThat(transactionalBuffer.getLargestFirstScn().equals(BigDecimal.ZERO));

        final String insertIntoATable = "insert into a table";
        final String anotherInsertIntoATable = "another insert into a table";
        final String duplicatedInsertIntoATable = "duplicated insert into a table";

        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, Instant.now(), insertIntoATable, (timestamp, smallestScn) -> { });
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, OTHER_SCN, Instant.now(), duplicatedInsertIntoATable, (timestamp, smallestScn) -> { });
        assertThat(transactionalBuffer.getLargestFirstScn().equals(OTHER_SCN));
        assertThat(transactionalBuffer.toString().contains(insertIntoATable));
        assertThat(transactionalBuffer.toString().contains(duplicatedInsertIntoATable));

        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, LARGEST_SCN, Instant.now(), insertIntoATable, (timestamp, smallestScn) -> { });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), duplicatedInsertIntoATable, (timestamp, smallestScn) -> { });
        assertThat(transactionalBuffer.getLargestFirstScn().equals(LARGEST_SCN));
        assertThat(transactionalBuffer.toString().contains(insertIntoATable));
        assertThat(transactionalBuffer.toString().contains(duplicatedInsertIntoATable));
        // make sure the duplications are OK in different transactions
        assertThat(transactionalBuffer.toString().indexOf(duplicatedInsertIntoATable) != transactionalBuffer.toString().lastIndexOf(duplicatedInsertIntoATable));

        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, Instant.now(), anotherInsertIntoATable, (timestamp, smallestScn) -> { });
        assertThat(transactionalBuffer.toString().contains(anotherInsertIntoATable));
        transactionalBuffer.rollback(OTHER_TRANSACTION_ID, "");

        // try to duplicate sql for the same transaction for the same SCN
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, OTHER_SCN, Instant.now(), duplicatedInsertIntoATable, (timestamp, smallestScn) -> { });

        assertThat(transactionalBuffer.toString().indexOf(duplicatedInsertIntoATable) == transactionalBuffer.toString().lastIndexOf(duplicatedInsertIntoATable));
        assertThat(transactionalBuffer.getLargestFirstScn().equals(LARGEST_SCN));
    }

}
