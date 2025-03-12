package com.wt.parquet.service;

import static com.wt.parquet.TestUtils.deleteDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@ExtendWith(MockitoExtension.class)
class ProcessServiceTest {

    private static final String JSON_FILTERED_ZIP_FILE_NAME = "Json_filtered.zip";
    private static final String JSON_FILTERED_FOLDER_NAME = "Json_filtered";
    private static final String TEST_VIEW_EVENT_FOLDER_NAME = "test-view_events";
    @Mock
    private DownloadService downloadService;
    @Mock
    private ExtractionService extractionService;
    @Mock
    private FilterService filterService;
    @Spy
    private ObjectMapper objectMapper;
    @InjectMocks
    private ProcessService processService;

    @BeforeEach
    void setUp() {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    }

    @AfterEach
    void cleanUp() {
        File folder = new File(JSON_FILTERED_FOLDER_NAME);
        File zipFile = new File(JSON_FILTERED_ZIP_FILE_NAME);
        if (folder.exists() && folder.isDirectory()) {
            deleteDirectory(folder.toPath());
        }
        if (zipFile.exists()) {
            zipFile.delete();
        }
    }


    @Test
    void processFileSuccessfully() throws Exception {
        //given
        ProcessService.FilterResult filterResult = new ProcessService.FilterResult(TEST_VIEW_EVENT_FOLDER_NAME,
                "2025-03-08");
        Map<String, Object> detailMap = new HashMap<>();
        detailMap.put("username", "test-user");
        detailMap.put("event_time", "2025-03-08 13:22:42.996452");
        List<Map<String, Object>> testData = new ArrayList<>();
        testData.add(detailMap);
        filterResult.data = testData;
        ProcessService.FileDetail fileDetail = new ProcessService.FileDetail("test_parquet_temp_11007639273718423191.parquet", 26);
        List<ProcessService.FileDetail> files = new ArrayList<>();
        files.add(fileDetail);
        filterResult.files = files;
        filterResult.editedDate = "2025-03-08";
        Map<String, ProcessService.FilterResult> filterResultMap = new HashMap<>();
        filterResultMap.put(TEST_VIEW_EVENT_FOLDER_NAME, filterResult);
        when(filterService.filterParquetFiles(any())).thenReturn(filterResultMap);

        //when
        processService.process();

        //then
        verify(objectMapper).writeValueAsString(any());
        File zipFile = new File(JSON_FILTERED_ZIP_FILE_NAME);
        assertTrue(zipFile.exists());
        boolean jsonFileFound = false;
        boolean manifestFileFound = false;
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                assertNotNull(entry, "ZIP should contain files");
                if (entry.getName().equalsIgnoreCase("manifest-20250308.txt")) {
                    manifestFileFound = true;
                } else if (entry.getName().equalsIgnoreCase("test-view_events-20250308.json")) {
                    jsonFileFound = true;
                }

            }
        }

        assertThat(jsonFileFound)
                .as("test-view_events-20250308.json file found in Json_filtered.zip")
                .isTrue();
        assertThat(manifestFileFound)
                .as("manifest-20250308.txt file found in Json_filtered.zip")
                .isTrue();

    }
}
