package com.real.matcher;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public interface Matcher {

  class IdMapping {

    private final int internalId;
    private final String externalId;

    public IdMapping(int internalId, String externalId) {
      this.internalId = internalId;
      this.externalId = externalId;
    }

    public int getInternalId() {
      return internalId;
    }

    public String getExternalId() {
      return externalId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null) {
        return false;
      }
      if (!(other instanceof IdMapping)) {
        return false;
      }

      IdMapping idMapping = (IdMapping) other;
      return Objects.equals(this.externalId, idMapping.externalId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(externalId);
    }
  }

  class CsvStream {

    private final String headerRow;
    private final Stream<String> dataRows;

    public CsvStream(String headerRow, Stream<String> dataRows) {
      this.headerRow = headerRow;
      this.dataRows = dataRows;
    }

    public String getHeaderRow() {
      return headerRow;
    }

    public Stream<String> getDataRows() {
      return dataRows;
    }
  }

  enum DatabaseType {
    XBOX, GOOGLE_PLAY, VUDU, AMAZON_INSTANT
  }

  enum MovieOrRole {
    MOVIE, ROLE
  }

  List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb);
}
