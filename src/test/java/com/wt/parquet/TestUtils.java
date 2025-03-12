package com.wt.parquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class TestUtils {
    
    public static void deleteDirectory(Path folderPath) {
        try {
            Files.walk(folderPath)
                    .sorted(Comparator.reverseOrder()) // Delete files first, then the folder
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
