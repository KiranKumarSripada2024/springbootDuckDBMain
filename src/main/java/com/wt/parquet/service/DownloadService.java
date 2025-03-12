package com.wt.parquet.service;

import com.wt.parquet.config.AppConfig;
import lombok.AllArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

@Service
@AllArgsConstructor
public class DownloadService {

    private final AppConfig appConfig;
    private final CloseableHttpClient httpClient;

    public File downloadZip() throws IOException {
        String date1 = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String zipUrl = appConfig.getDownloadUrl() + date1 + "&format=zip";
        File zipFile = new File(appConfig.getDownloadDir(), "insights_" + date1 + ".zip");

        // Taking the Username & Password from application.properties
        String auth = appConfig.getUsername() + ":" + appConfig.getPassword();
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

        HttpGet request = new HttpGet(zipUrl);
        request.addHeader("Authorization", "Basic " + encodedAuth);
        request.addHeader("Accept", "application/zip");

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getCode();
            if (statusCode == 200) {
                FileUtils.copyInputStreamToFile(response.getEntity().getContent(), zipFile);
                return zipFile;
            } else {
                try {
                    String responseMessage = EntityUtils.toString(response.getEntity());
                    throw new IOException("Failed to download ZIP: " + statusCode + " - " + responseMessage);
                } catch (ParseException e) {
                    throw new IOException("Failed to parse error response: " + e.getMessage(), e);
                }
            }
        }
    }


    @PreDestroy
    void cleanUp() {
        try {
            httpClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
