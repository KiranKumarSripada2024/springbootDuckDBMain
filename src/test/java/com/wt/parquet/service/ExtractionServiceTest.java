package com.wt.parquet.service;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class ExtractionServiceTest {

    @Test
    void extractParquetFromZipSuccessfully() throws IOException {
        //given
        File zipFile = Paths.get("src", "test", "resources", "test-parquets.zip").toFile();
        ExtractionService extractionService = new ExtractionService();
        //when
        Map<String, List<byte[]>> parquetFiles = extractionService.extractParquetFromZip(zipFile);
        //then
        assertThat(parquetFiles.values().size()).isEqualTo(1);

    }
}
