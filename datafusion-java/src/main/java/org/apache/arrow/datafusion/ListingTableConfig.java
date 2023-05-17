package org.apache.arrow.datafusion;

import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/** Configuration for creating a ListingTable */
public class ListingTableConfig extends AbstractProxy implements AutoCloseable {
  public static class Builder {
    private final String tablePath;
    private ListingOptions options = null;

    public Builder(String tablePath) {
      this.tablePath = tablePath;
    }

    public Builder withListingOptions(ListingOptions options) {
      this.options = options;
      return this;
    }

    /**
     * Create the listing table config. This is async as the schema may need to be inferred
     *
     * @return Future that will complete with the table config
     */
    public CompletableFuture<ListingTableConfig> build(SessionContext context) {
      return createListingTableConfig(this, context).thenApply(ListingTableConfig::new);
    }
  }

  public static Builder builder(Path tablePath) {
    return new Builder(tablePath.toString());
  }

  public static Builder builder(URI tablePath) {
    return new Builder(tablePath.toString());
  }

  private ListingTableConfig(long pointer) {
    super(pointer);
  }

  private static CompletableFuture<Long> createListingTableConfig(
      Builder builder, SessionContext context) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    Runtime runtime = context.getRuntime();
    create(
        runtime.getPointer(),
        context.getPointer(),
        builder.tablePath,
        builder.options == null ? 0 : builder.options.getPointer(),
        (errMessage, configId) -> {
          if (null != errMessage && !errMessage.equals("")) {
            future.completeExceptionally(new RuntimeException(errMessage));
          } else {
            future.complete(configId);
          }
        });
    return future;
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native void create(
      long runtime, long context, String tablePath, long options, ObjectResultCallback callback);

  private static native void destroy(long pointer);
}
