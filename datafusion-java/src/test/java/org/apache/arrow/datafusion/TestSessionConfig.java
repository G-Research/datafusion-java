package org.apache.arrow.datafusion;

import org.junit.jupiter.api.Test;

public class TestSessionConfig {
  @Test
  public void testCreateSessionWithConfig() throws Exception {
    try (SessionContext context =
        SessionContexts.withConfig((c) -> c.parquetOptions().withEnablePageIndex(true))) {
      // pass
    }
  }
}
