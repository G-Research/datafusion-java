package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.Test;

public class TestSessionConfig {
  @Test
  public void testCreateSessionWithConfig() throws Exception {
    try (SessionContext context =
        SessionContexts.withConfig((c) -> c.parquetOptions().withEnablePageIndex(true))) {
      // Only testing we can successfully create a session context with the config
    }
  }

  @Test
  public void testParquetOptions() throws Exception {
    try (SessionConfig config = new SessionConfig()) {
      ParquetOptions parquetOptions = config.parquetOptions();

      assertFalse(parquetOptions.enablePageIndex());
      parquetOptions.withEnablePageIndex(true);
      assertTrue(parquetOptions.enablePageIndex());

      assertTrue(parquetOptions.pruning());
      parquetOptions.withPruning(false);
      assertFalse(parquetOptions.pruning());

      assertTrue(parquetOptions.skipMetadata());
      parquetOptions.withSkipMetadata(false);
      assertFalse(parquetOptions.skipMetadata());

      assertFalse(parquetOptions.metadataSizeHint().isPresent());
      parquetOptions.withMetadataSizeHint(Optional.of(123L));
      Optional<Long> sizeHint = parquetOptions.metadataSizeHint();
      assertTrue(sizeHint.isPresent());
      assertEquals(123L, sizeHint.get());
      parquetOptions.withMetadataSizeHint(Optional.empty());
      assertFalse(parquetOptions.metadataSizeHint().isPresent());

      assertFalse(parquetOptions.pushdownFilters());
      parquetOptions.withPushdownFilters(true);
      assertTrue(parquetOptions.pushdownFilters());

      assertFalse(parquetOptions.reorderFilters());
      parquetOptions.withReorderFilters(true);
      assertTrue(parquetOptions.reorderFilters());
    }
  }

  @Test
  public void testSqlParserOptions() throws Exception {
    try (SessionConfig config = new SessionConfig()) {
      SqlParserOptions sqlParserOptions = config.sqlParserOptions();

      assertFalse(sqlParserOptions.parseFloatAsDecimal());
      sqlParserOptions.withParseFloatAsDecimal(true);
      assertTrue(sqlParserOptions.parseFloatAsDecimal());

      assertTrue(sqlParserOptions.enableIdentNormalization());
      sqlParserOptions.withEnableIdentNormalization(false);
      assertFalse(sqlParserOptions.enableIdentNormalization());

      assertEquals("generic", sqlParserOptions.dialect());
      sqlParserOptions.withDialect("PostgreSQL");
      assertEquals("PostgreSQL", sqlParserOptions.dialect());
    }
  }
}
