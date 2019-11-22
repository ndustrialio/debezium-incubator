/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Pustovetov
 * 
 * Transactional buffer is designed to register callbacks, to execute them when transaction commits and to clear them
 * when transaction rollbacks.
 */
@NotThreadSafe
public final class TransactionalBuffer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalBuffer.class);

    private final Map<String, Transaction> transactions;
    private final ExecutorService executor;
    private final AtomicInteger taskCounter;
    private final ErrorHandler errorHandler;
    private Optional<TransactionalBufferMetrics> metrics;
    private final Deque<OracleDmlParser> parsers = new ConcurrentLinkedDeque<>();
    private final Set<String> abandonedTransactionIds;


    /**
     * Constructor to create a new instance.
     *
     * @param logicalName logical name
     * @param errorHandler error handler
     * @param metrics metrics MBean exposed
     */
    TransactionalBuffer(String logicalName, ErrorHandler errorHandler, TransactionalBufferMetrics metrics) {
        this.transactions = new HashMap<>();
        this.executor = Threads.newSingleThreadExecutor(OracleConnector.class, logicalName, "transactional-buffer");
        this.taskCounter = new AtomicInteger();
        this.errorHandler = errorHandler;
        if (metrics != null) {
            this.metrics = Optional.of(metrics);
        } else {
            this.metrics = Optional.empty();
        }
        this.abandonedTransactionIds = new HashSet<>();
    }

    /**
     * Registers callback to execute when transaction commits.
     *
     * @param transactionId transaction identifier
     * @param scn SCN
     * @param changeTime time of DML parsing completion
     * @param redoSql statement from redo
     * @param callback callback to execute when transaction commits
     */
    void registerCommitCallback(String transactionId, BigDecimal scn, Instant changeTime, String redoSql, CommitCallback callback) {
        if (abandonedTransactionIds.contains(transactionId)) {
            LOGGER.error("Another DML for an abandoned transaction {} : {}", transactionId, redoSql);
            return;
        }

        metrics.ifPresent(TransactionalBufferMetrics::incrementCapturedDmlCounter);
        metrics.ifPresent(m -> m.setLagFromTheSource(changeTime));

        transactions.computeIfAbsent(transactionId, s -> new Transaction(scn)).commitCallbacks.add(callback);
        Transaction transaction = transactions.get(transactionId);
        if (transaction != null) {
            transaction.addRedoSql(redoSql);
        }
        metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
    }

    /**
     * @param transactionId transaction identifier
     * @param timestamp commit timestamp
     * @param context context to check that source is running
     * @param debugMessage todo delete
     * @return true if committed transaction is in the buffer
     */
    boolean commit(String transactionId, Timestamp timestamp, ChangeEventSource.ChangeEventSourceContext context, String debugMessage) {
        Transaction transaction = transactions.remove(transactionId);
        if (transaction == null) {
            return false;
        }
        taskCounter.incrementAndGet();
        abandonedTransactionIds.remove(transactionId);

        List<CommitCallback> commitCallbacks = transaction.commitCallbacks;
        BigDecimal smallestScn = transactions.isEmpty() ? null : calculateSmallestScn();
        LOGGER.trace("COMMIT, {}, smallest SCN: {}", debugMessage, smallestScn);
        executor.execute(() -> {
            try {
                for (CommitCallback callback : commitCallbacks) {
                    if (!context.isRunning()) {
                        return;
                    }
                    callback.execute(timestamp, smallestScn);
                    metrics.ifPresent(TransactionalBufferMetrics::incrementCommittedTransactions);
                }
            }
            catch (InterruptedException e) {
                LOGGER.error("Thread interrupted during running", e);
                Thread.currentThread().interrupt();
            }
            catch (Exception e) {
                errorHandler.setProducerThrowable(e);
            }
            finally {
                taskCounter.decrementAndGet();
                metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
                metrics.ifPresent(m -> m.incrementCommittedDmlCounter(commitCallbacks.size()));
            }
        });
        return true;
    }

    /**
     * It could happen that the first DML of a transaction will fall out of online redo logs range.
     * We don't mine archived logs, neither rely on continuous_mine configuration option.
     * Hence we have to address these cases manually.
     *
     * It is limited by  following condition:
     * allOnlineRedoLogFiles.size() - currentlyMinedLogFiles.size() <= 1
     *
     * If each redo lasts for 10 minutes and 7 redo group have been configured, any transaction cannot lasts longer than 1 hour.
     *
     * In case of an abandonment, all DMLs/Commits/Rollbacs for this transaction will be ignored
     *
     * In other words connector will not send any part of this transaction to Kafka
     * @param thresholdScn the smallest SVN of any transaction to keep in the buffer. All others will be removed.
     */
    void abandonLongTransactions(Long thresholdScn){
        BigDecimal threshold = new BigDecimal(thresholdScn);
        Iterator<Map.Entry<String, Transaction>> iter = transactions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Transaction> transaction = iter.next();
            if (transaction.getValue().firstScn.compareTo(threshold) <= 0) {
                LOGGER.warn("Following long running transaction will be abandoned and ignored: {} ", transaction.getValue().toString());
                abandonedTransactionIds.add(transaction.getKey());
                iter.remove();
                metrics.ifPresent(t -> t.addAbandonedTransactionId(transaction.getKey()));
                metrics.ifPresent(t -> t.decrementCapturedDmlCounter(transaction.getValue().commitCallbacks.size()));
                metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
            }
        }
    }

    private void parse(String transactionId){
        Transaction transaction = transactions.get(transactionId);
    } // todo - maybe parse on commit (saving on rolled back DML), disadvantage - delay on commit

    private BigDecimal calculateSmallestScn() {
        BigDecimal scn = transactions.values()
                .stream()
                .map(transaction -> transaction.firstScn)
                .min(BigDecimal::compareTo)
                .orElseThrow(() -> new DataException("Cannot calculate smallest SCN"));
        metrics.ifPresent(m -> m.setOldestScn(scn.longValue()));
        return scn;
    }

    /**
     * Clears registered callbacks for given transaction identifier.
     *
     * @param transactionId transaction id
     * @param debugMessage todo delete in the future
     * @return true if the rollback is for a transaction in the buffer
     */
    boolean rollback(String transactionId, String debugMessage) {
        Transaction transaction = transactions.remove(transactionId);
        if (transaction != null) {
            LOGGER.debug("Transaction {} rolled back", transactionId, debugMessage);
            abandonedTransactionIds.remove(transactionId);
            metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
            metrics.ifPresent(TransactionalBufferMetrics::incrementRolledBackTransactions);
            return true;
        }
        return false;
    }

    /**
     * Returns {@code true} if buffer is empty, otherwise {@code false}.
     *
     * @return {@code true} if buffer is empty, otherwise {@code false}
     */
    boolean isEmpty() {
        return transactions.isEmpty() && taskCounter.get() == 0;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        this.transactions.values().forEach(t -> result.append(t.toString()));
        return result.toString();
    }

    /**
     * Closes buffer.
     */
    void close() {
        transactions.clear();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("Thread interrupted during shutdown", e);
        }
    }

    /**
     * Callback is designed to execute when transaction commits.
     */
    public interface CommitCallback {

        /**
         * Executes callback.
         *
         * @param timestamp commit timestamp
         * @param smallestScn smallest SCN among other transactions
         */
        void execute(Timestamp timestamp, BigDecimal smallestScn) throws InterruptedException;
    }

    @NotThreadSafe
    private static final class Transaction {

        private final BigDecimal firstScn;
        private final List<CommitCallback> commitCallbacks;
        private final List<String> redoSqls;
        private boolean parsingComplete;

        private Transaction(BigDecimal firstScn) {
            this.firstScn = firstScn;
            this.commitCallbacks = new ArrayList<>();
            this.redoSqls = new ArrayList<>();
            parsingComplete = false;
        }

        private void addRedoSql(String redoSql){
            redoSqls.add(redoSql);
        }

        @Override
        public String toString() {
            StringBuilder result =  new StringBuilder("First SCN = " + firstScn + "\nRedo SQL:\n ");
            redoSqls.forEach(sql -> result.append(sql).append("\n"));
            return result.toString();
        }

        private void setStatus(boolean completed){
            this.parsingComplete = completed;
        }
        private boolean isCompleted(){
            return this.parsingComplete;
        }
    }
}
