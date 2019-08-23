/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;


import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.OracleChangeRecordValueConverter;
import io.debezium.connector.oracle.logminer.valueholder.ColumnValueHolder;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.ValueConverter;
import io.debezium.text.ParsingException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.debezium.connector.oracle.antlr.listener.ParserListenerUtils.getTableName;
/**
 * This class contains common methods for DML parser listeners
 */
abstract class BaseDmlParserListener<T> extends PlSqlParserBaseListener {

    protected String catalogName;
    protected String schemaName;
    protected Table table;
    final OracleChangeRecordValueConverter converter;
    protected String alias;

    protected OracleDmlParser parser;

    Map<T, ColumnValueHolder> newColumnValues = new LinkedHashMap<>();
    Map<T, ColumnValueHolder> oldColumnValues = new LinkedHashMap<>();

    BaseDmlParserListener(String catalogName, String schemaName, OracleDmlParser parser) {
        this.parser = parser;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.converter = parser.getConverters();
    }

    // Defines the key of the Map of ColumnValueHolder. It could be String or Integer
    abstract protected T getKey(Column column, int index);

    /**
     * This method prepares all column value placeholders, based on the table metadata
     * @param ctx DML table expression context
     */
    void init(PlSqlParser.Dml_table_expression_clauseContext ctx) {
        String tableName  = getTableName(ctx.tableview_name());
        table = parser.databaseTables().forTable(catalogName, schemaName, tableName);
        if (table == null) {
            throw new ParsingException(null, "Trying to parse a table, which does not exist.");
        }
        for (int i = 0; i < table.columns().size(); i++) {
            Column column = table.columns().get(i);
            int type = column.jdbcType();
            T key = getKey(column, i);
            String name = stripeQuotes(column.name().toUpperCase());
            newColumnValues.put(key, new ColumnValueHolder(new LogMinerColumnValueImpl(name, type)));
            oldColumnValues.put(key, new ColumnValueHolder(new LogMinerColumnValueImpl(name, type)));
        }
    }

    /**
     * This converts the given value to the appropriate object. The conversion is based on the column definition
     *
     * @param column column Object
     * @param value value object
     * @param converters given converter
     * @return object as the result of this conversion. It could be null if converter cannot build the schema
     * or if converter or value are null
     */
    Object convertValueToSchemaType(Column column, Object value, OracleChangeRecordValueConverter converters) {
        if (converters != null && value != null) {
            final SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
            if (schemaBuilder == null) {
                return null;
            }
            final Schema schema = schemaBuilder.build();
            final Field field = new Field(column.name(), 1, schema);
            final ValueConverter valueConverter = converters.converter(column, field);

            return valueConverter.convert(value);
        }
        return null;
    }

    /**
     * In some cases values of the parsed expression are enclosed in apostrophes.
     * Even null values are surrounded by single apostrophes. This method removes them.
     *
     * @param text supplied value which might be enclosed by apostrophes.
     * @return clean String or null in case if test = "null" or = "NULL"
     */
    String removeApostrophes(String text){
        if (text != null && text.indexOf("'") == 0 && text.lastIndexOf("'") == text.length()-1){
            return text.substring(1, text.length() -1);
        }
        if ("null".equalsIgnoreCase(text)){
            return null;
        }
        return text;
    }

    private String stripeQuotes(String text){
        if (text != null && text.indexOf("\"") == 0 && text.lastIndexOf("\"") == text.length()-1){
            return text.substring(1, text.length() -1);
        }
        return text;
    }

}