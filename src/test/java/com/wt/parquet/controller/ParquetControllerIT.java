package com.wt.parquet.controller;


import static com.wt.parquet.TestUtils.deleteDirectory;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import com.wt.parquet.AbstractMVCTestSupport;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;

class ParquetControllerIT extends AbstractMVCTestSupport {

    @MockitoBean
    private CloseableHttpClient httpClient;
    @Mock
    private CloseableHttpResponse mockedCloseableHttpResponse;
    @Mock
    private HttpEntity mockedHttpEntity;

    @AfterAll
    static void cleanup() {
        File Json_filtered_folder = new File("Json_filtered");
        if (Json_filtered_folder.exists() && Json_filtered_folder.isDirectory()) {
            deleteDirectory(Json_filtered_folder.toPath());
        }
        File test_zip_Download = new File("test_zip_Download");
        if (test_zip_Download.exists() && test_zip_Download.isDirectory()) {
            deleteDirectory(test_zip_Download.toPath());
        }

        File json_filteredZip = new File("Json_filtered.zip");
        if (json_filteredZip.exists()) {
            json_filteredZip.delete();
        }
        File duckDbFile = new File("duckdb_data.db");
        if (duckDbFile.exists()) {
            duckDbFile.delete();
        }
    }

    @Test
    void processParquetFilesSuccessfully() throws Exception {
        //given
        File zipFile = Paths.get("src", "test", "resources", "test-parquets.zip").toFile();
        FileInputStream fileInputStream = new FileInputStream((zipFile));
        when(httpClient.execute(any())).thenReturn(mockedCloseableHttpResponse);
        when(mockedCloseableHttpResponse.getCode()).thenReturn(200);
        when(mockedCloseableHttpResponse.getEntity()).thenReturn(mockedHttpEntity);
        when(mockedHttpEntity.getContent()).thenReturn(fileInputStream);
        //when  //then
        getMockMvc().perform(get("/api/parquet/process"))
                .andExpect(status().isOk());

        verify(httpClient).execute(any());

    }

}
