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
package edu.pitt.dbmi.i2b2.database;

import edu.pitt.dbmi.i2b2.database.service.CrcDBService;
import edu.pitt.dbmi.i2b2.database.service.FileSysService;
import edu.pitt.dbmi.i2b2.database.service.MetadataDBService;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * Mar 1, 2023 3:24:46 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
@SpringBootApplication
public class I2b2DatabaseApplication implements CommandLineRunner {

    @Autowired
    private MetadataDBService metadataDBService;

    @Autowired
    private CrcDBService crcDBService;

    @Autowired
    private FileSysService fileSysService;

    @Override
    public void run(String... args) throws Exception {
//        insertObservationFacts();
//        createMetadatTableForSharephe();
//        mergeMetadatFiles();
    }

    private void mergeMetadatFiles() {
        System.out.println("================================================================================");
        System.out.println("Merge Metadata Files");
        System.out.println("--------------------------------------------------------------------------------");
        Path metadataFileFolder = Paths.get("data", "metadata");
        fileSysService.mergeMetadataFiles(metadataFileFolder);
        System.out.println("================================================================================");
    }

    private void createMetadatTableForSharephe() {
        System.out.println("================================================================================");
        System.out.println("Create Sharephe Metadata Table");
        System.out.println("--------------------------------------------------------------------------------");
        Path metadataDirectory = Paths.get("data", "metadata");
        try {
            metadataDBService.createSharepheMetadataTables(metadataDirectory);
        } catch (Exception exception) {
            exception.printStackTrace(System.err);
        }
        System.out.println("================================================================================");
    }

    private void insertObservationFacts() {
        System.out.println("================================================================================");
        System.out.println("Insert Into Act Observation Fact Table");
        System.out.println("--------------------------------------------------------------------------------");
        Path observationFactFile = Paths.get("data", "observation_fact.tsv");
        try {
            crcDBService.insertIntoObservationFactTable(observationFactFile);
        } catch (SQLException | IOException exception) {
            exception.printStackTrace(System.err);
        }
        System.out.println("================================================================================");
    }

    public static void main(String[] args) {
        SpringApplication.run(I2b2DatabaseApplication.class, args);
    }

}
