package org.apache.arrow.datafusion;

import org.junit.jupiter.api.Test;

public class TestParquetFormat {
  @Test
  public void createParquetFormat() throws Exception {
    try (ParquetFormat format = new ParquetFormat()) {
      // pass
    }
  }
}
