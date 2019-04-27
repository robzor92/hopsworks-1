package io.hops.hopsworks.api.experiments;

import javax.ws.rs.BeanParam;
import javax.ws.rs.QueryParam;
import java.util.Set;

public class ExperimentsBeanParam {
  @QueryParam("filter_by")
  private Set<FilterBy> filter;
  @BeanParam
  private ExpansionBeanParam expansions;


  public ExperimentsBeanParam(@QueryParam("filter_by") Set<FilterBy> filter) {
    this.filter = filter;
  }

  public Set<FilterBy> getFilter() {
    return filter;
  }

  public void setFilter(Set<FilterBy> filter) {
    this.filter = filter;
  }

  public ExpansionBeanParam getExpansions() {
    return expansions;
  }

  public void setExpansions(ExpansionBeanParam expansions) {
    this.expansions = expansions;
  }
}
