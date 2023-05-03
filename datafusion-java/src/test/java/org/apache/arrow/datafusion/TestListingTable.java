package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestListingTable {
  @Test
  public void testCsvListingTable(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      Files.createDirectories(dataDir);

      Path csvFilePath0 = dataDir.resolve("0.csv");
      List<String> lines = Arrays.asList("x,y", "1,2", "3,4");
      Files.write(csvFilePath0, lines);

      Path csvFilePath1 = dataDir.resolve("1.csv");
      lines = Arrays.asList("x,y", "1,12", "3,14");
      Files.write(csvFilePath1, lines);

      try (CsvFormat format = new CsvFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format).withFileExtension(".csv").build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(dataDir)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        testQuery(context, allocator);
      }
    }
  }

  @Test
  public void testParquetListingTable(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      writeParquetFiles(dataDir);

      try (ParquetFormat format = new ParquetFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format).withFileExtension(".parquet").build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(dataDir)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        testQuery(context, allocator);
      }
    }
  }

  @Test
  public void testMultiplePaths(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path dataDir = tempDir.resolve("data");
      Path[] dataFiles = writeParquetFiles(dataDir);

      try (ParquetFormat format = new ParquetFormat();
          ListingOptions listingOptions =
              ListingOptions.builder(format).withFileExtension(".parquet").build();
          ListingTableConfig tableConfig =
              ListingTableConfig.builder(dataFiles)
                  .withListingOptions(listingOptions)
                  .build(context)
                  .join();
          ListingTable listingTable = new ListingTable(tableConfig)) {
        context.registerTable("test", listingTable);
        testQuery(context, allocator);
      }
    }
  }

  private static Path[] writeParquetFiles(Path dataDir) throws Exception {
    String schema =
        "{\"namespace\": \"org.example\","
            + "\"type\": \"record\","
            + "\"name\": \"record_name\","
            + "\"fields\": ["
            + " {\"name\": \"x\", \"type\": \"long\"},"
            + " {\"name\": \"y\", \"type\": \"long\"}"
            + " ]}";

    Path parquetFilePath0 = dataDir.resolve("0.parquet");
    ParquetWriter.writeParquet(
        parquetFilePath0,
        schema,
        2,
        (i, record) -> {
          record.put("x", i * 2 + 1);
          record.put("y", i * 2 + 2);
        });

    Path parquetFilePath1 = dataDir.resolve("1.parquet");
    ParquetWriter.writeParquet(
        parquetFilePath1,
        schema,
        2,
        (i, record) -> {
          record.put("x", i * 2 + 1);
          record.put("y", i * 2 + 12);
        });
    return new Path[] {parquetFilePath0, parquetFilePath1};
  }

  private static void testQuery(SessionContext context, BufferAllocator allocator)
      throws Exception {
    try (ArrowReader reader =
        context
            .sql("SELECT y FROM test WHERE x = 3 ORDER BY y")
            .thenComposeAsync(df -> df.collect(allocator))
            .join()) {

      long[] expectedResults = {4, 14};
      int globalRow = 0;
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        BigIntVector yValues = (BigIntVector) root.getVector(0);
        for (int row = 0; row < root.getRowCount(); ++row, ++globalRow) {
          assertTrue(globalRow < expectedResults.length);
          assertEquals(expectedResults[globalRow], yValues.get(row));
        }
      }
      assertEquals(expectedResults.length, globalRow);
    }
  }
}
