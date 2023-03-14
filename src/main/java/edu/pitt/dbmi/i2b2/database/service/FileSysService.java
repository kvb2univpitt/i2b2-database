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
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;

/**
 *
 * Mar 1, 2023 3:35:03 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
@Service
public class FileSysService {

    private final ResourcePatternResolver resourcePatternResolver;

    @Autowired
    public FileSysService(ResourcePatternResolver resourcePatternResolver) {
        this.resourcePatternResolver = resourcePatternResolver;
    }

    public void mergeMetadataFiles(Path metadataDir) {
        Set<String> lines = new LinkedHashSet<>();
        List<Path> metadataFiles = getMetadataFiles(metadataDir);
        for (Path metadataFile : metadataFiles) {
            try (BufferedReader reader = Files.newBufferedReader(metadataFile)) {
                reader.readLine();  // skip header
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    // skip blank line
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    lines.add(line);
                }
            } catch (IOException exception) {
                exception.printStackTrace(System.err);
            }
        }

        Path outputFile = Paths.get("/home", "kvb2", "shared", "tmp", "metadata.tsv");
        try (PrintStream writer = new PrintStream(Files.newOutputStream(outputFile, StandardOpenOption.CREATE))) {
            lines.forEach(writer::println);
        } catch (IOException exception) {
            exception.printStackTrace(System.err);
        }
    }

    public void mergeMetadataFiles2(Path metadataDir) {
        List<Path> metadataFiles = getMetadataFiles(metadataDir);
        Path outputFile = Paths.get("/home", "kvb2", "shared", "tmp", "metadata.tsv");
        try (PrintStream writer = new PrintStream(Files.newOutputStream(outputFile, StandardOpenOption.CREATE))) {
            for (Path metadataFile : metadataFiles) {
                try (BufferedReader reader = Files.newBufferedReader(metadataFile)) {
                    // skip header
                    reader.readLine();

                    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                        // skip blank line
                        if (line.trim().isEmpty()) {
                            continue;
                        }

                        writer.println(line);
                    }
                }
            }
        } catch (IOException exception) {
            exception.printStackTrace(System.err);
        }
    }

    public List<Path> getMetadataFiles(Path metadataFileDir) {
        try {
            return Files.list(metadataFileDir)
                    .filter(Files::isRegularFile)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException exception) {
            exception.printStackTrace(System.err);
        }

        return Collections.EMPTY_LIST;
    }

    public String getResourceFileContents(Path file) throws IOException {
        List<String> list = new LinkedList<>();

        Resource resource = resourcePatternResolver.getResource("classpath:/" + file.toString());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                line = line.trim();
                if (!line.isEmpty()) {
                    list.add(line);
                }
            }
        }

        return list.stream()
                .collect(Collectors.joining());
    }

    public List<String> getResourceFileContentByLines(Path file) throws IOException {
        List<String> list = new LinkedList<>();

        Resource resource = resourcePatternResolver.getResource("classpath:/" + file.toString());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                line = line.trim();
                if (!line.isEmpty()) {
                    list.add(line);
                }
            }
        }

        return list;
    }

    public List<Path> listFiles(Path dir) throws IOException {
        return Files.list(dir)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
    }

    /**
     * Get the file header (the first line of the file).
     *
     * @param file
     * @return
     * @throws IOException
     */
    public List<String> getHeaders(Path file) throws IOException {
        Optional<String> header = Files.lines(file).findFirst();
        if (header.isPresent()) {
            return Arrays.stream(Delimiters.TAB.split(header.get()))
                    .map(String::trim)
                    .filter(e -> !e.isEmpty())
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
        } else {
            return Collections.EMPTY_LIST;
        }
    }

}
