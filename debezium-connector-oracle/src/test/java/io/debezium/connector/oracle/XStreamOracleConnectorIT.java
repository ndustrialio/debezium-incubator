/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * This class subclasses OracleConnectorIT tests for XStream adapter
 *
 */
public class XStreamOracleConnectorIT extends OracleConnectorIT {

    @BeforeClass
    public static void beforeSuperClass() throws SQLException {
        connection = TestHelper.testConnection();

        builder = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER");
        OracleConnectorIT.beforeClass();
    }

    @Test
    public void shouldTakeSnapshot() throws Exception {
        super.shouldTakeSnapshot();
    }

    @Test
    public void shouldContinueWithStreamingAfterSnapshot() throws Exception {
        super.shouldContinueWithStreamingAfterSnapshot();
    }

    @Test
    public void shouldStreamTransaction() throws Exception {
        super.shouldStreamTransaction();
    }

    @Test
    public void shouldStreamAfterRestart() throws Exception {
        super.shouldStreamAfterRestart(0L);
    }

    @Test
    public void shouldStreamAfterRestartAfterSnapshot() throws Exception {
        super.shouldStreamAfterRestartAfterSnapshot();
    }

    @Test
    public void shouldReadChangeStreamForExistingTable() throws Exception {
        super.shouldReadChangeStreamForExistingTable(1000L);
    }

    @Test
    public void shouldReadChangeStreamForTableCreatedWhileStreaming() throws Exception {
        super.shouldReadChangeStreamForTableCreatedWhileStreaming();
    }

    @Test
    public void shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable() throws Exception {
        super.shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable();
    }

    @Test
    public void deleteWithoutTombstone() throws Exception {
        super.deleteWithoutTombstone();
    }
}
