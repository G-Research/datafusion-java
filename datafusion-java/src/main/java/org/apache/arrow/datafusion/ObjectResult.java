package org.apache.arrow.datafusion;

/** Helper class for receiving results from the native library that might fail */
final class ObjectResult {
  private long objectId;
  private String errorMessage = null;

  public long getObjectId() throws RuntimeException {
    if (errorMessage != null) {
      throw new RuntimeException(errorMessage);
    }
    return objectId;
  }

  public void setError(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public void setOk(long objectId) {
    this.objectId = objectId;
  }
}
