package org.apache.arrow.datafusion;

/** The Apache Arrow IPC file format configuration. This format is also known as Feather V2 */
public class ArrowFormat extends AbstractProxy implements FileFormat {
  /** Create a new ArrowFormat with default options */
  public ArrowFormat() {
    super(create());
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native long create();

  private static native void destroy(long pointer);
}
