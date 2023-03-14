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
import edu.pitt.dbmi.i2b2.database.util.DateFormatters;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Metadata Database Service
 *
 * Mar 1, 2023 3:33:41 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
@Service
public class MetadataDBService extends AbstractDBService {

    private final JdbcTemplate metadataJdbcTemplate;

    @Autowired
    public MetadataDBService(JdbcTemplate metadataJdbcTemplate, FileSysService fileSysService) {
        super(fileSysService);
        this.metadataJdbcTemplate = metadataJdbcTemplate;
    }

    public void createSharepheMetadataTables(Path metadataDirectory) throws SQLException, IOException {
        String tableName = "sharephe_metadata";
        createOntologyTable(metadataJdbcTemplate, tableName);
        for (Path metadataFile : fileSysService.getMetadataFiles(metadataDirectory)) {
            insertIntoOntologyTable(metadataJdbcTemplate, tableName, metadataFile);
        }
        createOntologyTableIndices(metadataJdbcTemplate, "shp", tableName);
    }

    private void createOntologyTableIndices(JdbcTemplate jdbcTemplate, String indexNameprefix, String tableName) throws SQLException, IOException {
        createTableIndexes(jdbcTemplate, indexNameprefix, tableName, Paths.get("metadata", "metadata_table_indices.sql"));
    }

    private void insertIntoOntologyTable(JdbcTemplate jdbcTemplate, String tableName, Path file) throws SQLException, IOException {
        batchInsertMetadata(jdbcTemplate, tableName, DEFAULT_BATCH_SIZE, file, Delimiters.TAB, DateFormatters.METADATA_DATE_FORMATTER);
    }

    protected void createMetadataTable(JdbcTemplate jdbcTemplate, String tableName, Path file) throws SQLException, IOException {
        String query = fileSysService.getResourceFileContents(file);

        jdbcTemplate.execute(query.replaceAll("I2B2", tableName));
    }

    private void createOntologyTable(JdbcTemplate jdbcTemplate, String tableName) throws SQLException, IOException {
        switch (getDatabaseVendor(jdbcTemplate)) {
            case "PostgreSQL":
                createMetadataTable(jdbcTemplate, tableName, Paths.get("metadata", "postgresql", "metadata_table.sql"));
                break;
            case "Oracle":
                createMetadataTable(jdbcTemplate, tableName, Paths.get("metadata", "oracle", "metadata_table.sql"));
                break;
            case "Microsoft SQL Server":
                createMetadataTable(jdbcTemplate, tableName, Paths.get("metadata", "sqlserver", "metadata_table.sql"));
                break;
        }
    }

}
