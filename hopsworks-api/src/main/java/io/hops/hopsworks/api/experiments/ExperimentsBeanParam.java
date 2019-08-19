package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.jobs.SortBy;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.BeanParam;
import javax.ws.rs.QueryParam;
import java.util.LinkedHashSet;
import java.util.Set;

public class ExperimentsBeanParam {
  @QueryParam("sort_by")
  @ApiParam(value = "ex. sort_by=date_created:desc,name:asc",
      allowableValues = "id:asc,id:desc,name:asc,name:desc,date_created:asc,date_created:desc,jobtype:asc," +
          "jobtype:desc,creator:asc,creator:desc,creator_firstname:asc,creator_firstname:desc," +
          "creator_lastname:asc,creator_lastname:desc,state:asc,state:desc,finalstatus:asc,finalstatus:desc,progress:asc," +
          "progress:desc,submissiontime:asc,submissiontime:desc,duration:asc,duration:desc")
  private String sortBy;
  private final Set<SortBy> sortBySet;
  @QueryParam("filter_by")
  private Set<FilterBy> filter;
  @BeanParam
  private ExpansionBeanParam expansions;

  private Set<SortBy> getSortBy(String param) {
    if (param == null || param.isEmpty()) {
      return new LinkedHashSet<>();
    }
    String[] params = param.split(",");
    //Hash table and linked list implementation of the Set interface, with predictable iteration order
    Set<SortBy> sortBys = new LinkedHashSet<>();//make ordered
    SortBy sort;
    for (String s : params) {
      sort = new SortBy(s.trim());
      sortBys.add(sort);
    }
    return sortBys;
  }


  public ExperimentsBeanParam(@QueryParam("filter_by") Set<FilterBy> filter, @QueryParam("sort_by") String sortBy) {


    this.filter = filter;
    this.sortBy = sortBy;
    sortBySet = getSortBy()
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
