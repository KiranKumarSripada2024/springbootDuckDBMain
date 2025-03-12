package com.wt.parquet.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Value("${app.download.url}")
    private String downloadUrl;

    @Value("${app.download.dir}")
    private String downloadDir;

    @Value("${app.json.dir1}")
    private String jsonDir1;

    @Value("${app.json.dir2}")
    private String jsonDir2;

    @Value("${app.duckdb.file}")
    private String duckDbFile;

    @Value("${app.username}")
    private String username;
    @Value("${app.password}")
    private String password;

    public String getPassword() {
        return password;
    }

    public String getDownloadUrl() {
        return downloadUrl;
    }

    public String getDownloadDir() {
        return downloadDir;
    }

    public String getJsonDir1() {
        return jsonDir1;
    }

    public String getJsonDir2() {
        return jsonDir2;
    }

    public String getDuckDbFile() {
        return duckDbFile;
    }

    public String getUsername() {
        return username;
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        return objectMapper;
    }

    @Bean
    public CloseableHttpClient httpClient() {
        return HttpClients.createDefault();
    }


}

