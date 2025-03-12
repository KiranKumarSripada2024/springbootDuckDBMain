package com.wt.parquet.service;

import static com.wt.parquet.TestUtils.deleteDirectory;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import com.wt.parquet.config.AppConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@ExtendWith(MockitoExtension.class)
class DownloadServiceTest {

    private static final String TEST_DIR = "test-dir";
    @Mock
    private AppConfig appConfig;
    @Mock
    private CloseableHttpClient httpClient;
    @Mock
    private CloseableHttpResponse mockCloseableHttpResponse;
    @InjectMocks
    private DownloadService downloadService;

    @AfterAll
    static void cleanUp() {
        File folder = new File(TEST_DIR);
        if (folder.exists() && folder.isDirectory()) {
            deleteDirectory(folder.toPath());
        }
    }

    @Test
    void downloadSuccessfully() throws IOException {
        //given
        HttpEntity httpEntity = new StringEntity("{\"message\": \"Success\"}", StandardCharsets.UTF_8);
        when(httpClient.execute(any())).thenReturn(mockCloseableHttpResponse);
        when(mockCloseableHttpResponse.getCode()).thenReturn(200);
        when(mockCloseableHttpResponse.getEntity()).thenReturn(httpEntity);
        when(appConfig.getDownloadDir()).thenReturn(TEST_DIR);
        //when
        File actualFile = downloadService.downloadZip();
        //then
        assertTrue(actualFile.exists());
    }


    @Test
    void throwIOExceptionIfDownloadUnsuccessful() throws IOException {
        //given
        HttpEntity httpEntity = new StringEntity("{\"message\": \"failure\"}", StandardCharsets.UTF_8);
        when(httpClient.execute(any())).thenReturn(mockCloseableHttpResponse);
        when(mockCloseableHttpResponse.getCode()).thenReturn(404);
        when(mockCloseableHttpResponse.getEntity()).thenReturn(httpEntity);
        //where
        //then
        assertThrows(IOException.class, () ->
                downloadService.downloadZip());
    }

}
