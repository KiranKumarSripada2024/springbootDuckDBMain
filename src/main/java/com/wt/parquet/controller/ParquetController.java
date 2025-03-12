package com.wt.parquet.controller;

import com.wt.parquet.service.ProcessInitialLoadService;
import com.wt.parquet.service.ProcessService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/parquet")
@AllArgsConstructor
public class ParquetController {

    private final ProcessService processService;
    private final ProcessInitialLoadService processInitialLoadService;

    @GetMapping("/process")
    public String processParquetFiles() {
        try {
            processService.process();
            return "Processing completed!";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    @GetMapping("/initialLoad")
    public String processInitialLoadFiles() {
        try {
            processInitialLoadService.process();
            return "Initial load processing completed for the ZIP file ";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }


    @GetMapping("/initialLoad-manual")
    public String processInitialLoadCustomZip(@RequestParam String zipPath) {
        try {
            processInitialLoadService.processCustomZip(zipPath);
            return "Initial load processing completed for custom ZIP: " + zipPath;
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

}

