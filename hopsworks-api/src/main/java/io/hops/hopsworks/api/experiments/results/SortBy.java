package io.hops.hopsworks.api.experiments.results;

import io.hops.hopsworks.common.dao.AbstractFacade;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class SortBy {

  private final Sorts sortBy;
  private final AbstractFacade.OrderBy param;

  public SortBy(String param) {
    String[] sortByParams = param.split(":");
    String sort = "";
    try {
      sort = sortByParams[0].toUpperCase();
      this.sortBy = Sorts.valueOf(sort);
    } catch (IllegalArgumentException iae) {
      throw new WebApplicationException("Sort by need to set a valid sort parameter, but found: " + sort,
          Response.Status.NOT_FOUND);
    }
    String order = "";
    try {
      order = sortByParams.length > 1 ? sortByParams[1].toUpperCase() : this.sortBy.getDefaultParam();
      this.param = AbstractFacade.OrderBy.valueOf(order);
    } catch (IllegalArgumentException iae) {
      throw new WebApplicationException("Sort by " + sort + " need to set a valid order(asc|desc), but found: " + order
          , Response.Status.NOT_FOUND);
    }
  }

  public String getValue() {
    return this.sortBy.getValue();
  }

  public AbstractFacade.OrderBy getParam() {
    return this.param;
  }

  public enum Sorts {
    OPTIMIZATION_KEY("OPTIMIZATION_KEY", "ASC");
    private final String value;
    private final String defaultParam;

    Sorts(String value, String defaultParam) {
      this.value = value;
      this.defaultParam = defaultParam;
    }

    public String getValue() {
      return value;
    }

    public String getDefaultParam() {
      return defaultParam;
    }

    @Override
    public String toString() {
      return value;
    }

  }
}