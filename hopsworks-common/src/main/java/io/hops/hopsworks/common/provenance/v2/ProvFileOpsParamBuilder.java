/*
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.common.provenance.v2;

import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class ProvFileOpsParamBuilder {
  private Map<String, List<Pair<ProvQuery.Field, Object>>> fileOpsFilter = new HashMap<>();
  private List<Pair<ProvQuery.Field, SortOrder>> fileOpsSortBy = new ArrayList<>();
  private Set<ProvQuery.FileExpansions> expansions = new HashSet<>();
  private Map<String, List<Pair<ProvQuery.Field, Object>>> appStateFilter = new HashMap<>();
  private Pair<Integer, Integer> pagination = null;
  
  public ProvFileOpsParamBuilder withQueryParamFilter(Set<String> params) throws GenericException {
    for(String param : params) {
      ParamBuilder.addToFilters(fileOpsFilter, ProvQuery.extractFilter(param, ProvQuery.QueryType.QUERY_FILE_OP));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamSortBy(List<String> params) throws GenericException {
    for(String param : params) {
      fileOpsSortBy.add(ProvQuery.extractSort(param, ProvQuery.QueryType.QUERY_FILE_STATE));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamExpansions(Set<String> params) throws GenericException {
    for(String param : params) {
      try {
        expansions.add(ProvQuery.FileExpansions.valueOf(param));
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "param " + param + " not supported - supported params:"
            + EnumSet.allOf(ProvQuery.FileExpansions.class),
          "exception extracting FilterBy param", e);
      }
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamAppExpansionFilter(Set<String> params) throws GenericException {
    for(String param : params) {
      ParamBuilder.addToFilters(appStateFilter,
        ProvQuery.extractFilter(param, ProvQuery.QueryType.QUERY_EXPANSION_APP));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withPagination(Integer offset, Integer limit) {
    Integer o;
    Integer l;
    if (offset == null && limit == null) {
      pagination = null;
      return this;
    }
    if(offset == null) {
      o = 0;
    } else if(offset < 0) {
      o = 0;
    } else {
      o = offset;
    }
    if(o > ElasticController.MAX_PAGE_SIZE) {
      o = ElasticController.MAX_PAGE_SIZE;
    }
    if(limit == null) {
      l = ElasticController.DEFAULT_PAGE_SIZE;
    } else if(limit < 0) {
      l = ElasticController.DEFAULT_PAGE_SIZE;
    } else {
      l = limit;
    }
    if(o+l > ElasticController.MAX_PAGE_SIZE) {
      l = ElasticController.MAX_PAGE_SIZE - o;
    }
    pagination = Pair.with(o, l);
    return this;
  }
  
  public boolean hasPagination() {
    return pagination != null;
  }
  
  public Pair<Integer, Integer> getPagination() {
    return pagination;
  }
  
  public boolean hasAppExpansion() {
    return expansions.contains(ProvQuery.FileExpansions.APP);
  }
  
  public ProvFileOpsParamBuilder withAppExpansion() {
    expansions.add(ProvQuery.FileExpansions.APP);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAppExpansion(String appId) {
    withAppExpansion();
    ParamBuilder.addToFilters(appStateFilter,
      Pair.with(ProvQuery.ExpansionApp.APP_ID, appId));
    return this;
  }
  
  public Map<String, List<Pair<ProvQuery.Field, Object>>> getFileOpsFilter() {
    return fileOpsFilter;
  }
  
  public List<Pair<ProvQuery.Field, SortOrder>> getFileOpsSortBy() {
    return fileOpsSortBy;
  }
  
  public Map<String, List<Pair<ProvQuery.Field, Object>>> getAppStateFilter() {
    return appStateFilter;
  }
  
  public ProvFileOpsParamBuilder withProjectInodeId(Long projectInodeId) {
    ParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvQuery.FileOps.PROJECT_I_ID, projectInodeId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileInodeId(Long fileInodeId) {
    ParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvQuery.FileOps.FILE_I_ID, fileInodeId));
    return this;
  }
  
  
  public ProvFileOpsParamBuilder withAppId(String appId) {
    ParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvQuery.FileOps.APP_ID, appId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileOperation(ProvFileOps fileOp) {
    ParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvQuery.FileOps.FILE_OPERATION, fileOp.name()));
    return this;
  }
}
