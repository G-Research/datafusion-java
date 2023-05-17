package org.apache.arrow.datafusion;

/** Configures options for a ListingTable */
public class ListingOptions extends AbstractProxy implements AutoCloseable {
  public static class Builder {
    private final FileFormat format;
    private String fileExtension = "";

    public Builder(FileFormat format) {
      this.format = format;
    }

    public Builder withFileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
      return this;
    }

    public ListingOptions build() {
      return new ListingOptions(this);
    }
  }

  public static Builder builder(FileFormat format) {
    return new Builder(format);
  }

  private ListingOptions(Builder builder) {
    super(create(builder.format.getPointer(), builder.fileExtension));
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native long create(long format, String fileExtension);

  private static native void destroy(long pointer);
}
