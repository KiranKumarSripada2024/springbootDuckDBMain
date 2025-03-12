package com.wt.parquet;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = {ParquetTestApplication.class})
public abstract class AbstractMVCTestSupport {

    @Autowired
    private MockMvc mockMvc;


    protected final MockMvc getMockMvc() {
        return mockMvc;
    }
}
