package com.wt.parquet.service;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@ExtendWith(MockitoExtension.class)
class FilterServiceTest {

    private Map<String, List<byte[]>> inputParquetFiles;

    @BeforeEach
    void setup() throws IOException {
        File zipFile = Paths.get("src", "test", "resources", "test-parquets.zip").toFile();
        inputParquetFiles = extractParquetFromZip(zipFile);
    }

    private Map<String, List<byte[]>> extractParquetFromZip(File zipFile) throws IOException {
        Map<String, List<byte[]>> parquetFiles = new HashMap<>();

        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().endsWith(".parquet")) {
                    String folder = entry.getName().split("/")[0];

                    // Read parquet file into memory as byte array
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = zis.read(buffer)) != -1) {
                        baos.write(buffer, 0, bytesRead);
                    }

                    parquetFiles.computeIfAbsent(folder, k -> new ArrayList<>()).add(baos.toByteArray());
                }
            }
        }
        return parquetFiles;
    }

    @Test
    void filterParquetFilesSuccessfully() {

        FilterService filterService = new FilterService();

        Map<String, ProcessService.FilterResult> filterResultMap = filterService.filterParquetFiles(inputParquetFiles);

        assertThat(filterResultMap.values())
                .flatExtracting(filterResult -> filterResult.files)
                .isNotEmpty();

        assertThat(filterResultMap.values())
                .extracting(filterResult -> filterResult.folderName)
                .isNotEmpty();


    }
}
