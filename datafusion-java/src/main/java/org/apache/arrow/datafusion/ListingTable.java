package org.apache.arrow.datafusion;

/** A data source composed of multiple files that share a schema */
public class ListingTable extends AbstractProxy implements TableProvider {
  public ListingTable(ListingTableConfig config) {
    super(createListingTable(config));
  }

  private static long createListingTable(ListingTableConfig config) {
    ObjectResult result = new ObjectResult();
    create(config.getPointer(), result);
    return result.getObjectId();
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native void create(long config, ObjectResult result);

  private static native void destroy(long pointer);
}
