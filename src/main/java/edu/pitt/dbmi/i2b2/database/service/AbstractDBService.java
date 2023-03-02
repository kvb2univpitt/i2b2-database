/*
 * Copyright (C) 2023 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.pitt.dbmi.i2b2.database.service;

import edu.pitt.dbmi.i2b2.database.Delimiters;
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Mar 1, 2023 3:29:59 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public abstract class AbstractDBService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDBService.class);

    protected static final int DEFAULT_BATCH_SIZE = 10000;

    protected static final DateFormat ONTOLOGY_DATE_FORMATTER = new SimpleDateFormat("dd-MMM-yy");
    protected static final DateFormat SHAREPHE_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.000");

    protected final FileSysService fileSysService;

    public AbstractDBService(FileSysService fileSysService) {
        this.fileSysService = fileSysService;
    }

    protected boolean isTableExist(DataSource dataSource, String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement pstmt = null;
            switch (conn.getMetaData().getDatabaseProductName().toLowerCase()) {
                case "postgresql":
                    pstmt = conn.prepareStatement("SELECT 1 FROM pg_tables WHERE schemaname = ? AND (tablename = UPPER(?) OR tablename = LOWER(?))");
                    pstmt.setString(1, conn.getSchema());
                    pstmt.setString(2, tableName);
                    pstmt.setString(3, tableName);
                    break;
                case "oracle":
                    pstmt = conn.prepareStatement("SELECT 1 FROM user_tables WHERE table_name = UPPER(?) OR table_name = LOWER(?)");
                    pstmt.setString(1, tableName);
                    pstmt.setString(2, tableName);
                    break;
                case "microsoft sql server":
                    pstmt = conn.prepareStatement("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND (table_name = UPPER(?) OR table_name = LOWER(?))");
                    pstmt.setString(1, conn.getSchema());
                    pstmt.setString(2, tableName);
                    pstmt.setString(3, tableName);
                    break;
            }

            if (pstmt != null) {
                ResultSet resultSet = pstmt.executeQuery();

                return resultSet.next();
            }
        }

        return false;
    }

    protected Set<String> getUniqueColumnDataFromTable(DataSource dataSource, String table, String column) throws SQLException {
        return new HashSet<>(getColumnDataFromTable(dataSource, table, column));
    }

    protected List<String> getColumnDataFromTable(DataSource dataSource, String table, String column) throws SQLException {
        List<String> data = new LinkedList<>();

        try (Connection conn = dataSource.getConnection()) {
            String query = String.format("SELECT %s FROM %s.%s", column, conn.getSchema(), table.toLowerCase());
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                data.add(rs.getString(1).trim().toLowerCase());
            }
        } catch (Exception exception) {
            LOGGER.error("Failed to get column data from table.", exception);
            throw new SQLException(exception);
        }

        return data;
    }

    protected void deleteFromTableByColumnValue(DataSource dataSource, String table, String columnName, String columnValue, int columnType) throws SQLException, IOException {
        try (Connection conn = dataSource.getConnection()) {
            int[] columnTypes = {columnType};
            String[] values = {columnValue};

            String sql = createDeletePreparedStatement(conn.getSchema(), table.toLowerCase(), columnName);
            PreparedStatement stmt = conn.prepareStatement(sql);
            try {
                setColumns(stmt, columnTypes, values);
                stmt.execute();
            } catch (Exception exception) {
                LOGGER.error("Failed to delete from table by column value.", exception);
                throw new SQLException(exception);
            }
        }
    }

    protected void insertIntoTable(DataSource dataSource, String table, Path file) throws SQLException, IOException {
        try (Connection conn = dataSource.getConnection()) {
            // create prepared statement
            String sql = createInsertPreparedStatement(conn.getSchema(), table.toLowerCase(), fileSysService.getHeaders(file));
            PreparedStatement stmt = conn.prepareStatement(sql);

            // get columnTypes
            int[] columnTypes = getColumnTypes(stmt.getParameterMetaData());

            Files.lines(file)
                    .skip(1)
                    .filter(line -> !line.trim().isEmpty())
                    .map(Delimiters.TAB::split)
                    .forEach(values -> {
                        try {
                            setColumns(stmt, columnTypes, values);

                            // add null columns not provided
                            if (values.length < columnTypes.length) {
                                for (int i = values.length; i < columnTypes.length; i++) {
                                    stmt.setNull(i + 1, Types.NULL);
                                }
                            }

                            stmt.execute();
                        } catch (Exception exception) {
                            LOGGER.error("", exception);
                        }
                    });
        }
    }

    protected void batchInsertIntoTable(DataSource dataSource, String table, Path file, int batchSize) throws SQLException, IOException {
        try (Connection conn = dataSource.getConnection()) {
            // create prepared statement
            String sql = createInsertPreparedStatement(conn.getSchema(), table.toLowerCase(), fileSysService.getHeaders(file));
            PreparedStatement stmt = conn.prepareStatement(sql);

            // get columnTypes
            int[] columnTypes = getColumnTypes(stmt.getParameterMetaData());

            int count = 0;
            try (BufferedReader reader = Files.newBufferedReader(file)) {
                // skip header
                reader.readLine();

                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    // skip blank line
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    String[] values = Delimiters.TAB.split(line);
                    try {
                        setColumns(stmt, columnTypes, values);

                        // add null columns not provided
                        if (values.length < columnTypes.length) {
                            for (int i = values.length; i < columnTypes.length; i++) {
                                stmt.setNull(i + 1, Types.NULL);
                            }
                        }

                        stmt.addBatch();
                        count++;
                    } catch (Exception exception) {
                        LOGGER.error("", exception);
                    }

                    if (count == batchSize) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }

            if (count > 0) {
                stmt.executeBatch();
                stmt.clearBatch();
                count = 0;
            }
        }
    }

    protected void setColumns(PreparedStatement stmt, int[] columnTypes, String[] values) throws SQLException, ParseException, NumberFormatException {
        for (int i = 0; i < values.length; i++) {
            int columnIndex = i + 1;
            String value = values[i].trim();
            if (value.isEmpty()) {
                stmt.setNull(columnIndex, Types.NULL);
            } else {
                switch (columnTypes[i]) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        stmt.setString(columnIndex, value);
                        break;
                    case Types.TINYINT:
                        stmt.setByte(columnIndex, Byte.parseByte(value));
                        break;
                    case Types.SMALLINT:
                        stmt.setShort(columnIndex, Short.parseShort(value));
                        break;
                    case Types.INTEGER:
                        stmt.setInt(columnIndex, Integer.parseInt(value));
                        break;
                    case Types.BIGINT:
                        stmt.setLong(columnIndex, Long.parseLong(value));
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                        stmt.setFloat(columnIndex, Float.parseFloat(value));
                        break;
                    case Types.DOUBLE:
                        stmt.setDouble(columnIndex, Double.parseDouble(value));
                        break;
                    case Types.NUMERIC:
                        stmt.setBigDecimal(columnIndex, new BigDecimal(value));
                        break;
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                        stmt.setDate(columnIndex, new Date(SHAREPHE_DATE_FORMATTER.parse(value).getTime()));
                        break;
                    case Types.BIT:
                        stmt.setBoolean(columnIndex, value.equals("1"));
                        break;
                    case Types.VARBINARY:
                    case Types.BINARY:
                        stmt.setBytes(columnIndex, value.getBytes());
                        break;
                }
            }
        }
    }

    protected int[] getColumnTypes(ParameterMetaData metadata) throws SQLException {
        int[] types = new int[metadata.getParameterCount()];
        for (int i = 0; i < types.length; i++) {
            types[i] = metadata.getParameterType(i + 1);
        }

        return types;
    }

    public String getSchema(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return conn.getSchema();
        }
    }

    public String getDatabaseVendor(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return conn.getMetaData().getDatabaseProductName();
        }
    }

    protected String createInsertPreparedStatement(String schema, String tableName, List<String> columnNames) {
        String columns = columnNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.joining(","));
        String placeholder = IntStream.range(0, columnNames.size())
                .mapToObj(e -> "?")
                .collect(Collectors.joining(","));

        return String.format("INSERT INTO %s.%s (%s) VALUES (%s)", schema, tableName, columns, placeholder);
    }

    protected String createDeletePreparedStatement(String schema, String tableName, String columnName) {
        return String.format("DELETE FROM  %s.%s WHERE %s = ?", schema, tableName, columnName);
    }

}
