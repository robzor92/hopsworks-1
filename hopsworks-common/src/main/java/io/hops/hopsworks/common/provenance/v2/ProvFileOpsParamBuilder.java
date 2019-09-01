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

import io.hops.hopsworks.common.provenance.util.ElasticPaginationChecker;
import io.hops.hopsworks.exceptions.GenericException;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProvFileOpsParamBuilder {
  private Map<String, ProvFileQuery.FilterVal> fileOpsFilter = new HashMap<>();
  private List<Pair<ProvFileQuery.Field, SortOrder>> fileOpsSortBy = new ArrayList<>();
  private Set<ProvFileQuery.FileExpansions> expansions = new HashSet<>();
  private Map<String, ProvFileQuery.FilterVal> appStateFilter = new HashMap<>();
  private Pair<Integer, Integer> pagination = null;
  
  public ProvFileOpsParamBuilder withQueryParamFilterBy(Set<String> params) throws GenericException {
    ProvParamBuilder.withFilterBy(fileOpsFilter, params, ProvFileQuery.QueryType.QUERY_FILE_OP);
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamSortBy(List<String> params) throws GenericException {
    for(String param : params) {
      fileOpsSortBy.add(ProvFileQuery.extractSort(param, ProvFileQuery.QueryType.QUERY_FILE_OP));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamExpansions(Set<String> params) throws GenericException {
    ProvParamBuilder.withExpansions(expansions, params);
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamAppExpansionFilter(Set<String> params) throws GenericException {
    for(String param : params) {
      ProvParamBuilder.addToFilters(appStateFilter,
        ProvFileQuery.extractFilter(param, ProvFileQuery.QueryType.QUERY_EXPANSION_APP));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withPagination(Integer offset, Integer limit) throws GenericException {
    ElasticPaginationChecker.checkPagination(offset, limit);
    pagination = Pair.with(offset, limit);
    return this;
  }
  
  public boolean hasPagination() {
    return pagination != null;
  }
  
  public Pair<Integer, Integer> getPagination() {
    return pagination;
  }
  
  public boolean hasAppExpansion() {
    return expansions.contains(ProvFileQuery.FileExpansions.APP);
  }
  
  public ProvFileOpsParamBuilder withAppExpansion() {
    expansions.add(ProvFileQuery.FileExpansions.APP);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAppExpansion(String appId) throws GenericException {
    withAppExpansion();
    ProvParamBuilder.addToFilters(appStateFilter,
      Pair.with(ProvFileQuery.ExpansionApp.APP_ID, appId));
    return this;
  }
  
  public Map<String, ProvFileQuery.FilterVal> getFileOpsFilter() {
    return fileOpsFilter;
  }
  
  public List<Pair<ProvFileQuery.Field, SortOrder>> getFileOpsSortBy() {
    return fileOpsSortBy;
  }
  
  public Map<String, ProvFileQuery.FilterVal> getAppStateFilter() {
    return appStateFilter;
  }
  
  public ProvFileOpsParamBuilder withProjectInodeId(Long projectInodeId) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvFileQuery.FileOps.PROJECT_I_ID, projectInodeId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileInodeId(Long fileInodeId) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, fileInodeId));
    return this;
  }
  
  
  public ProvFileOpsParamBuilder withAppId(String appId) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvFileQuery.FileOps.APP_ID, appId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileOperation(ProvFileOps fileOp) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilter, Pair.with(ProvFileQuery.FileOps.FILE_OPERATION, fileOp.name()));
    return this;
  }
}
