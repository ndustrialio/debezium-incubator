/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnectorConfig;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.fest.assertions.Assertions.assertThat;

public class LogMinerUtilsTest {

    private static final BigDecimal SCN = BigDecimal.ONE;
    private static final BigDecimal OTHER_SCN = BigDecimal.TEN;
    private static final Logger LOGGER = Logger.getLogger(LogMinerUtilsTest.class.getName());

    @Test
    public void testStartLogMinerStatement() {
        String statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS"));
        assertThat(statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING"));
        assertThat(!statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"));
        assertThat(!statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE"));
        statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, false);
        assertThat(!statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS"));
        assertThat(!statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING"));
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"));
        assertThat(!statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE"));
        statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, true);
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS"));
        assertThat(!statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING"));
        assertThat(!statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"));
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE"));
        statement = SqlUtils.getStartLogMinerStatement(SCN.longValue(), OTHER_SCN.longValue(), OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true);
        assertThat(!statement.contains("DBMS_LOGMNR.DICT_FROM_REDO_LOGS"));
        assertThat(!statement.contains("DBMS_LOGMNR.DDL_DICT_TRACKING"));
        assertThat(statement.contains("DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"));
        assertThat(statement.contains("DBMS_LOGMNR.CONTINUOUS_MINE"));
    }

    @Test
    public void testDuration() throws Exception {
        AtomicReference<Duration> lagFromTheSource = new AtomicReference<>();
        Instant changeTime = Instant.now();
        Thread.sleep(100);
        lagFromTheSource.set(Duration.between(changeTime, Instant.now()));
        LOGGER.info("time: "+ lagFromTheSource.get().toMillis());

        Double value = Double.parseDouble("18446744073709551615");
        value = Double.parseDouble("12333");
        Long value1 = 12334L;
        if (value > value1) {
            String s = "say hello";
        }
    }

}
