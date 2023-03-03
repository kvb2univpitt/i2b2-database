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

import edu.pitt.dbmi.i2b2.database.util.DateFormatters;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Data Repository (CRC) Database Service
 *
 * Mar 1, 2023 5:57:25 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
@Service
public class CrcDBService extends AbstractDBService {

    private static final String OBSERVATION_FACT_TABLE = "observation_fact";
    private static final String CONCEPT_DIMENSION_TABLE = "concept_dimension";

    private final JdbcTemplate crcJdbcTemplate;

    @Autowired
    public CrcDBService(JdbcTemplate crcJdbcTemplate, FileSysService fileSysService) {
        super(fileSysService);
        this.crcJdbcTemplate = crcJdbcTemplate;
    }

    public void insertIntoObservationFactTable(Path file) throws SQLException, IOException {
        batchInsertIntoTable(
                crcJdbcTemplate.getDataSource(),
                OBSERVATION_FACT_TABLE,
                file,
                DEFAULT_BATCH_SIZE,
                DateFormatters.OBSERVATION_FACTS_DATE_FORMATTER);
    }

}
