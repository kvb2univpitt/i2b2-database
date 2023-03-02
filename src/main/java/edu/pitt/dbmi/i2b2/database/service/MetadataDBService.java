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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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

    public void createSharepheMetadataTable(Path metadataDir) {
        List<Path> metadataFiles = getMetadataFiles(metadataDir);
        if (!metadataFiles.isEmpty()) {
            String tableName = "sharephe_metadata";
            try {
                createMetadataTable(tableName);
                for (Path metadataFile : metadataFiles) {
                    batchInsertIntoTable(metadataJdbcTemplate.getDataSource(), tableName, metadataFile, DEFAULT_BATCH_SIZE);
                }
                createMetadataTableIndices(tableName.toLowerCase(), tableName);
            } catch (IOException | SQLException exception) {
                exception.printStackTrace(System.err);
            }
        }
    }

    private List<Path> getMetadataFiles(Path metadataFileDir) {
        try {
            return Files.list(metadataFileDir)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        } catch (IOException exception) {
            exception.printStackTrace(System.err);
        }

        return Collections.EMPTY_LIST;
    }

    private void createMetadataTableIndices(String indexSubfix, String tableName) throws SQLException, IOException {
        Path metadataIndexFile = Paths.get("metadata", "metadata_table_indices.sql");
        List<String> queries = fileSysService.getResourceFileContentByLines(metadataIndexFile);
        for (String query : queries) {
            query = query
                    .replaceAll(";", "")
                    .replaceAll("i2b2", indexSubfix)
                    .replaceAll("I2B2", tableName)
                    .trim();
            metadataJdbcTemplate.execute(query);
        }
    }

    private void createMetadataTable(String tableName) throws SQLException, IOException {
        switch (getDatabaseVendor(metadataJdbcTemplate.getDataSource()).toLowerCase()) {
            case "postgresql":
                createMetadataTableFromFile(tableName, Paths.get("metadata", "postgresql", "metadata_table.sql"));
                break;
            case "oracle":
                createMetadataTableFromFile(tableName, Paths.get("metadata", "oracle", "metadata_table.sql"));
                break;
            case "microsoft sql server":
                createMetadataTableFromFile(tableName, Paths.get("metadata", "sqlserver", "metadata_table.sql"));
                break;
        }
    }

    protected void createMetadataTableFromFile(String tableName, Path file) throws SQLException, IOException {
        String query = fileSysService.getResourceFileContents(file);

        metadataJdbcTemplate.execute(query.replaceAll("I2B2", tableName));
    }

}
