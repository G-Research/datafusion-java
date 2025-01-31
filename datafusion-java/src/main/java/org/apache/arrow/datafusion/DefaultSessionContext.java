package org.apache.arrow.datafusion;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultSessionContext extends AbstractProxy implements SessionContext {

  private static final Logger logger = LoggerFactory.getLogger(DefaultSessionContext.class);

  static native void querySql(
      long runtime, long context, String sql, ObjectResultCallback callback);

  static native void registerCsv(
      long runtime, long context, String name, String path, Consumer<String> callback);

  static native void registerParquet(
      long runtime, long context, String name, String path, Consumer<String> callback);

  static native String registerTable(long context, String table_reference, long tableProvider);

  @Override
  public CompletableFuture<DataFrame> sql(String sql) {
    long runtime = getRuntime().getPointer();
    CompletableFuture<DataFrame> future = new CompletableFuture<>();
    querySql(
        runtime,
        getPointer(),
        sql,
        (errMessage, dataframeId) -> {
          if (null != errMessage && !errMessage.equals("")) {
            future.completeExceptionally(new RuntimeException(errMessage));
          } else {
            DefaultDataFrame frame = new DefaultDataFrame(DefaultSessionContext.this, dataframeId);
            future.complete(frame);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> registerCsv(String name, Path path) {
    long runtime = getRuntime().getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    registerCsv(
        runtime,
        getPointer(),
        name,
        path.toAbsolutePath().toString(),
        (errMessage) -> voidCallback(future, errMessage));
    return future;
  }

  @Override
  public CompletableFuture<Void> registerParquet(String name, Path path) {
    long runtime = getRuntime().getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    registerParquet(
        runtime,
        getPointer(),
        name,
        path.toAbsolutePath().toString(),
        (errMessage) -> voidCallback(future, errMessage));
    return future;
  }

  @Override
  public void registerTable(String table_reference, TableProvider tableProvider) {
    String errorMessage = registerTable(getPointer(), table_reference, tableProvider.getPointer());
    if (errorMessage != null && !errorMessage.isEmpty()) {
      throw new RuntimeException(errorMessage);
    }
  }

  private void voidCallback(CompletableFuture<Void> future, String errMessage) {
    if (null != errMessage && !errMessage.equals("")) {
      future.completeExceptionally(new RuntimeException(errMessage));
    } else {
      future.complete(null);
    }
  }

  @Override
  public Runtime getRuntime() {
    return runtime;
  }

  private final TokioRuntime runtime;

  DefaultSessionContext(long pointer) {
    super(pointer);
    this.runtime = TokioRuntime.create();
    registerChild(runtime);
  }

  @Override
  void doClose(long pointer) throws Exception {
    SessionContexts.destroySessionContext(pointer);
  }
}
