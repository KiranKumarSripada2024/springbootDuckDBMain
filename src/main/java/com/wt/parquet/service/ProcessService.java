package com.wt.parquet.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
@AllArgsConstructor
public class ProcessService {
    private static final Logger logger = LoggerFactory.getLogger(ProcessService.class);
    private static final String JSON_DIR = "Json_filtered";
    private static final String MANIFEST_FILE = "manifest.txt";
    private static final String ZIP_FILE_NAME = JSON_DIR + ".zip";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final ObjectMapper objectMapper;
    private final DownloadService downloadService;


    private final ExtractionService extractionService;
    private final FilterService filterService;


    public void process() throws Exception {
        // Step 1: Download ZIP file
        File zipFile = downloadService.downloadZip();

        // Step 2: Extract Parquet files into memory (byte arrays)
        Map<String, List<byte[]>> parquetFiles = extractionService.extractParquetFromZip(zipFile);

        // Step 3: Filter data using DuckDB (directly from memory)
        Map<String, FilterResult> filteredResults = filterService.filterParquetFiles(parquetFiles);

        // Step 4: Ensure Json_filtered directory exists
        File jsonDir = new File(JSON_DIR);
        if (!jsonDir.exists() && jsonDir.mkdirs()) {
            logger.info("Json_filtered directory created: {}", jsonDir.getAbsolutePath());
        }

        // Step 5: Save JSON output and generate manifest.txt
        saveJsonOutput(filteredResults);
        generateManifest(filteredResults);

        // Step 6: Zip the Json_filtered directory
        zipJsonFilteredDirectory();
    }

    private void saveJsonOutput(Map<String, FilterResult> filteredResults) {
        for (Map.Entry<String, FilterResult> entry : filteredResults.entrySet()) {
            String folderName = entry.getKey();
            FilterResult result = entry.getValue();
            String formattedDate = LocalDate.parse(result.editedDate).format(DATE_FORMATTER);
            File jsonOutputFile = new File(JSON_DIR, folderName + "-" + formattedDate + ".json");

            try (FileWriter writer = new FileWriter(jsonOutputFile)) {
                if (!result.data.isEmpty()) {
                    List<Map<String, Object>> removeData = new ArrayList<>();
                    for (Map<String, Object> row : result.data) {
                        Map<String, Object> removedRow = new HashMap<>(row);
                        removedRow.remove("filter_date");
                        removeData.add(removedRow);
                    }
                    writer.write(objectMapper.writeValueAsString(removeData));
                } else {
                    writer.write("");
                }
                logger.info("JSON saved: {}", jsonOutputFile.getAbsolutePath());
            } catch (IOException e) {
                logger.error("Error writing JSON file: {} - {}", jsonOutputFile.getName(), e.getMessage());
            }
        }
    }

    private void generateManifest(Map<String, FilterResult> filteredResults) {
        Optional<String> optionalEditedDate = filteredResults.values().stream()
                .map(result -> result.editedDate)
                .filter(Objects::nonNull)
                .findFirst();
        if (optionalEditedDate.isPresent()) {
            String formattedDate = LocalDate.parse(optionalEditedDate.get()).format(DATE_FORMATTER);
            String manifestFileName = "manifest-" + formattedDate + ".txt";
            File manifestFile = new File(JSON_DIR, manifestFileName);

            try (FileWriter writer = new FileWriter(manifestFile)) {
                for (Map.Entry<String, FilterResult> entry : filteredResults.entrySet()) {
                    FilterResult result = entry.getValue();
                    writer.write(result.folderName + "|" + formattedDate + "|" + result.totalFilteredRows + "\n");
                }
                logger.info("Manifest file saved: {}", manifestFile.getAbsolutePath());
            } catch (IOException e) {
                logger.error("Error writing {}: {}", manifestFileName, e.getMessage());
            }
        } else {
            logger.error("Error: No valid edited_date found for manifest naming.");
        }
    }

    private void zipJsonFilteredDirectory() {
        try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(ZIP_FILE_NAME))) {
            Path sourceDirPath = Paths.get(JSON_DIR);
            Files.walk(sourceDirPath).forEach(path -> {
                try {
                    String fileName = sourceDirPath.relativize(path).toString();
                    if (!fileName.isEmpty()) {
                        zipOut.putNextEntry(new ZipEntry(fileName));
                        if (!Files.isDirectory(path)) {
                            Files.copy(path, zipOut);
                        }
                        zipOut.closeEntry();
                    }
                } catch (IOException e) {
                    logger.error("Error zipping file: {} - {}", path, e.getMessage());
                }
            });
            logger.info("Zipped JSON directory: " + ZIP_FILE_NAME);
        } catch (IOException e) {
            logger.error("Error creating ZIP file: {}", e.getMessage());
        }
    }

    public static class FilterResult {
        public String folderName;
        public String editedDate;
        public int totalFilteredRows;
        public List<Map<String, Object>> data = new ArrayList<>();
        public List<FileDetail> files = new ArrayList<>();

        public FilterResult(String folderName, String editedDate) {
            this.folderName = folderName;
            this.editedDate = editedDate;
            this.totalFilteredRows = 0;
        }

        public void addData(Map<String, Object> row) {
            this.data.add(row);
        }

        public void addFile(String fileName, int recordCount) {
            this.files.add(new FileDetail(fileName, recordCount));
            this.totalFilteredRows += recordCount;
        }
    }

    public static class FileDetail {
        public String file;
        public int recordCount;

        public FileDetail(String file, int recordCount) {
            this.file = file;
            this.recordCount = recordCount;
        }
    }
}
