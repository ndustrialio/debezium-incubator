/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleOffsetContext;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Andrey Pustovetov
 * <p>
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
    private final Set<String> abandonedTransactionIds;

    // storing rolledBackTransactionIds is for debugging purposes to check what was rolled back to research, todo delete in future releases
    private final Set<String> rolledBackTransactionIds;

    // It holds the latest captured uncommitted SCN.
    // This number tracks starting point for the next mining cycle.
    // This gets increased by 1 on each COMMIT or ROLLBACK to avoid reading COMMITTED or ROLLEDBACK transactions on the next mining loop.
    private BigDecimal largestScn;

    /**
     * Constructor to create a new instance.
     *
     * @param logicalName  logical name
     * @param errorHandler error handler
     * @param metrics      metrics MBean exposed
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
        largestScn = BigDecimal.ZERO;
        this.abandonedTransactionIds = new HashSet<>();
        this.rolledBackTransactionIds = new HashSet<>();
    }

    /**
     * @return largest last SCN in the buffer among all transactions
     */
    public BigDecimal getLargestScn() {
        return largestScn;
    }

    /**
     * @return rolled back transactions
     */
    public Set<String> getRolledBackTransactionIds() {
        return new HashSet<>(rolledBackTransactionIds);
    }

    /**
     * Reset Largest SCN
     */
    public void resetLargestScn(Long value) {
        if (value != null) {
            largestScn = new BigDecimal(value);
        } else {
            largestScn = BigDecimal.ZERO;
        }
    }

    /**
     * Registers callback to execute when transaction commits.
     *
     * @param transactionId transaction identifier
     * @param scn           SCN
     * @param changeTime    time of DML parsing completion
     * @param redoSql       statement from redo
     * @param callback      callback to execute when transaction commits
     */
    void registerCommitCallback(String transactionId, BigDecimal scn, Instant changeTime, String redoSql, CommitCallback callback) {
        if (abandonedTransactionIds.contains(transactionId)) {
            LOGGER.warn("Another DML for an abandoned transaction {} : {}, ignored", transactionId, redoSql);
            return;
        }

        transactions.computeIfAbsent(transactionId, s -> new Transaction(scn));

        metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
        metrics.ifPresent(TransactionalBufferMetrics::incrementCapturedDmlCounter);
        metrics.ifPresent(m -> m.setLagFromTheSource(changeTime));

        // The transaction object is not a lightweight object anymore having all REDO_SQL stored.
        Transaction transaction = transactions.get(transactionId);
        if (transaction != null) {

            // todo this should never happen, delete when tested and confirmed
            if (rolledBackTransactionIds.contains(transactionId)) {
                LOGGER.debug("Ignore DML for rolled back transaction: SCN={}, REDO_SQL={}", scn, redoSql);
                return;
            }

            List<String> redoSqls = transaction.redoSqlMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
            if (redoSqls.contains(redoSql)) {
                LOGGER.debug("Ignored duplicated capture as of SCN={}, REDO_SQL={}", scn, redoSql);
                return;
            }

            transaction.commitCallbacks.add(callback);
            transaction.addRedoSql(scn, redoSql);
        }

        if (scn.compareTo(largestScn) > 0) {
            largestScn = scn;
        }
    }

    /**
     * @param transactionId transaction identifier
     * @param commitScn SCN of the commit
     * @param offsetContext Oracle offset
     * @param timestamp     commit timestamp
     * @param context       context to check that source is running
     * @param debugMessage  todo delete
     * @return true if committed transaction is in the buffer and was not processed already
     */
    boolean commit(String transactionId, BigDecimal commitScn, OracleOffsetContext offsetContext, Timestamp timestamp,
                   ChangeEventSource.ChangeEventSourceContext context, String debugMessage) {

        Transaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            return false;
        }

        transaction.lastScn = transaction.lastScn.add(BigDecimal.ONE);
        calculateLargestScn();

        transaction = transactions.remove(transactionId);
        BigDecimal smallestScn = calculateSmallestScn();

        taskCounter.incrementAndGet();
        abandonedTransactionIds.remove(transactionId);

        if (offsetContext.getCommitScn() != null && offsetContext.getCommitScn() >= commitScn.longValue()) {
            LOGGER.info("Transaction {} was already processed. Committed SCN in offset is {}, commit SCN of the transaction is {}",
                    transactionId, offsetContext.getCommitScn(), commitScn);
            metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
            return false;
        }

        List<CommitCallback> commitCallbacks = transaction.commitCallbacks;
        LOGGER.debug("COMMIT, {}, smallest SCN: {}, largest SCN {}", debugMessage, smallestScn, largestScn);
        executor.execute(() -> {
            try {
                int counter = commitCallbacks.size();
                for (CommitCallback callback : commitCallbacks) {
                    if (!context.isRunning()) {
                        return;
                    }
                    callback.execute(timestamp, smallestScn, commitScn, --counter);
                }

                metrics.ifPresent(TransactionalBufferMetrics::incrementCommittedTransactions);
                metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
                metrics.ifPresent(m -> m.incrementCommittedDmlCounter(commitCallbacks.size()));
                metrics.ifPresent(m -> m.setCommittedScn(commitScn.longValue()));
            } catch (InterruptedException e) {
                LOGGER.error("Thread interrupted during running", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                errorHandler.setProducerThrowable(e);
            } finally {
                taskCounter.decrementAndGet();
            }
        });

        return true;
    }

    /**
     * If for some reason the connector got restarted, the offset will point to the beginning of the oldest captured transaction.
     * Taking in consideration offset flush interval, the offset could be even older.
     * If that transaction was lasted for a long time, let say > 30 minutes, the offset will be not accessible after restart,
     * because we don't mine archived logs, neither rely on continuous_mine configuration option.
     * Hence we have to address these cases manually.
     * <p>
     * It is limited by  following condition:
     * allOnlineRedoLogFiles.size() - currentlyMinedLogFiles.size() <= 1
     * <p>
     * If each redo lasts for 10 minutes and 7 redo group have been configured, any transaction cannot lasts longer than 1 hour.
     * <p>
     * In case of an abandonment, all DMLs/Commits/Rollbacs for this transaction will be ignored
     * <p>
     * In other words connector will not send any part of this transaction to Kafka
     *
     * @param thresholdScn the smallest SVN of any transaction to keep in the buffer. All others will be removed.
     */
    void abandonLongTransactions(Long thresholdScn) {
        BigDecimal threshold = new BigDecimal(thresholdScn);
        Iterator<Map.Entry<String, Transaction>> iter = transactions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Transaction> transaction = iter.next();
            if (transaction.getValue().firstScn.compareTo(threshold) <= 0) {
                LOGGER.warn("Following long running transaction will be abandoned and ignored: {} ", transaction.getValue().toString());
                abandonedTransactionIds.add(transaction.getKey());
                iter.remove();

                calculateLargestScn();
                calculateSmallestScn();

                metrics.ifPresent(t -> t.addAbandonedTransactionId(transaction.getKey()));
                metrics.ifPresent(t -> t.decrementCapturedDmlCounter(transaction.getValue().commitCallbacks.size()));
                metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
            }
        }
    }

    private BigDecimal calculateSmallestScn() {
        BigDecimal scn = transactions.isEmpty() ? null : transactions.values()
                .stream()
                .map(transaction -> transaction.firstScn)
                .min(BigDecimal::compareTo)
                .orElseThrow(() -> new DataException("Cannot calculate smallest SCN"));
        metrics.ifPresent(m -> m.setOldestScn(scn == null ? -1 : scn.longValue()));
        return scn;
    }

    private void calculateLargestScn() {
        largestScn = transactions.isEmpty() ? BigDecimal.ZERO : transactions.values()
                .stream()
                .map(transaction -> transaction.lastScn)
                .max(BigDecimal::compareTo)
                .orElseThrow(() -> new DataException("Cannot calculate largest SCN"));
    }

    /**
     * Clears registered callbacks for given transaction identifier.
     *
     * @param transactionId transaction id
     * @param debugMessage  todo delete in the future
     * @return true if the rollback is for a transaction in the buffer
     */
    boolean rollback(String transactionId, String debugMessage) {

        Transaction transaction = transactions.get(transactionId);
        if (transaction != null) {
            LOGGER.debug("Transaction {} rolled back, {}", transactionId, debugMessage);

            transaction.lastScn = transaction.lastScn.add(BigDecimal.ONE);
            calculateLargestScn();

            transactions.remove(transactionId);
            calculateSmallestScn();

            abandonedTransactionIds.remove(transactionId);
            rolledBackTransactionIds.add(transactionId);

            metrics.ifPresent(m -> m.setActiveTransactions(transactions.size()));
            metrics.ifPresent(TransactionalBufferMetrics::incrementRolledBackTransactions);
            metrics.ifPresent(m -> m.addRolledBackTransactionId(transactionId));

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
        } catch (InterruptedException e) {
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
         * @param timestamp   commit timestamp
         * @param smallestScn smallest SCN among other transactions
         * @param commitScn commit SCN
         * @param callbackNumber number of the callback in the transaction
         */
        void execute(Timestamp timestamp, BigDecimal smallestScn, BigDecimal commitScn, int callbackNumber) throws InterruptedException;
    }

    @NotThreadSafe
    private static final class Transaction {

        private final BigDecimal firstScn;
        // this is SCN candidate, not actual COMMITTED_SCN
        private BigDecimal lastScn;
        private final List<CommitCallback> commitCallbacks;
        private final NavigableMap<BigDecimal, List<String>> redoSqlMap;

        private Transaction(BigDecimal firstScn) {
            this.firstScn = firstScn;
            this.commitCallbacks = new ArrayList<>();
            this.redoSqlMap = new TreeMap<>();
            this.lastScn = firstScn;
        }

        private void addRedoSql(BigDecimal scn, String redoSql) {
            this.lastScn = scn;

            List<String> sqlList = redoSqlMap.get(scn);
            if (sqlList == null) {
                redoSqlMap.put(scn, new ArrayList<>(Collections.singletonList(redoSql)));
            } else {
                sqlList.add(redoSql);
            }
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "firstScn=" + firstScn +
                    ", lastScn=" + lastScn +
                    ", redoSqls=" + Arrays.toString(redoSqlMap.values().toArray()) +
                    '}';
        }
    }
}
