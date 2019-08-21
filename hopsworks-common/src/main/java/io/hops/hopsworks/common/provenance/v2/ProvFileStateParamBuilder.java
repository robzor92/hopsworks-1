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
import io.hops.hopsworks.common.provenance.Provenance;
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

public class ProvFileStateParamBuilder {
  private Map<String, List<Pair<ProvQuery.Field, Object>>> fileStateFilter = new HashMap<>();
  private List<Pair<ProvQuery.Field, SortOrder>> fileStateSortBy = new ArrayList<>();
  private Map<String, String> exactXAttrFilter = new HashMap<>();
  private Map<String, String> likeXAttrFilter = new HashMap<>();
  private List<Pair<String, SortOrder>> xAttrSortBy = new ArrayList<>();
  private Set<ProvQuery.FileExpansions> expansions = new HashSet<>();
  private Map<String, List<Pair<ProvQuery.Field, Object>>> appStateFilter = new HashMap<>();
  private Pair<Integer, Integer> pagination = null;
  
  public ProvFileStateParamBuilder withQueryParamFileStateFilterBy(Set<String> params) throws GenericException {
    for(String param : params) {
      ParamBuilder.addToFilters(fileStateFilter,
        ProvQuery.extractFilter(param, ProvQuery.QueryType.QUERY_FILE_STATE));
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamFileStateSortBy(List<String> params) throws GenericException {
    for(String param : params) {
      fileStateSortBy.add(ProvQuery.extractSort(param, ProvQuery.QueryType.QUERY_FILE_STATE));
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamExactXAttr(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> p = ProvQuery.extractXAttrParam(param);
      exactXAttrFilter.put(p.getValue0(), p.getValue1());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamLikeXAttr(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> p = ProvQuery.extractXAttrParam(param);
      likeXAttrFilter.put(p.getValue0(), p.getValue1());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamXAttrSortBy(List<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> xattr = ProvQuery.extractXAttrParam(param);
      SortOrder order;
      try {
        order = SortOrder.valueOf(xattr.getValue1());
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "sort order " + xattr.getValue1() + " not supported - supported:"
            + EnumSet.allOf(SortOrder.class),
          "exception extracting FilterBy param", e);
      }
      xAttrSortBy.add(Pair.with(xattr.getValue0(), order));
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamExpansions(Set<String> params) throws GenericException {
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
  
  public ProvFileStateParamBuilder withQueryParamAppExpansionFilter(Set<String> params) throws GenericException {
    for(String param : params) {
      ParamBuilder.addToFilters(appStateFilter,
        ProvQuery.extractFilter(param, ProvQuery.QueryType.QUERY_EXPANSION_APP));
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withPagination(Integer offset, Integer limit) {
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
  
  public Map<String, List<Pair<ProvQuery.Field, Object>>> getFileStateFilter() {
    return fileStateFilter;
  }
  
  public List<Pair<ProvQuery.Field, SortOrder>> getFileStateSortBy() {
    return fileStateSortBy;
  }
  
  public Map<String, String> getExactXAttrFilter() {
    return exactXAttrFilter;
  }
  
  public Map<String, String> getLikeXAttrFilter() {
    return likeXAttrFilter;
  }
  
  public List<Pair<String, SortOrder>> getXAttrSortBy() {
    return xAttrSortBy;
  }
  
  public Set<ProvQuery.FileExpansions> getExpansions() {
    return expansions;
  }
  
  public Map<String, List<Pair<ProvQuery.Field, Object>>> getAppStateFilter() {
    return appStateFilter;
  }
  
  public Pair<Integer, Integer> getPagination() {
    return pagination;
  }
  
  public ProvFileStateParamBuilder withProjectInodeId(Long projectInodeId) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.PROJECT_I_ID, projectInodeId));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileInodeId(Long inodeId) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.FILE_I_ID, inodeId));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileName(String fileName) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.FILE_NAME, fileName));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileNameLike(String fileName) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileStateAux.FILE_NAME_LIKE, fileName));
    return this;
  }
  
  public ProvFileStateParamBuilder withUserId(String userId) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.USER_ID, userId));
    return this;
  }
  
  public ProvFileStateParamBuilder createdBefore(Long timestamp) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileStateAux.CREATE_TIMESTAMP_LT, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder createdAfter(Long timestamp) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileStateAux.CREATE_TIMESTAMP_GT, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder createdOn(Long timestamp) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.CREATE_TIMESTAMP, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder withAppId(String appId) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.APP_ID, appId));
    return this;
  }
  
  public ProvFileStateParamBuilder withMlId(String mlId) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.ML_ID, mlId));
    return this;
  }
  
  public ProvFileStateParamBuilder withMlType(String mlType) {
    ParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvQuery.FileState.ML_TYPE, mlType));
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrs(Map<String, String> xAttrs) {
    for(Map.Entry<String, String> xAttr : xAttrs.entrySet()) {
      withXAttr(xAttr.getKey(), xAttr.getValue());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttr(String key, String val) {
    String xattrKey = ProvQuery.processXAttrKey(key);
    exactXAttrFilter.put(xattrKey, val);
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrsLike(Map<String, String> xAttrs) {
    for(Map.Entry<String, String> xAttr : xAttrs.entrySet()) {
      withXAttrLike(xAttr.getKey(), xAttr.getValue());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrLike(String key, String val) {
    String xattrKey = ProvQuery.processXAttrKey(key);
    likeXAttrFilter.put(xattrKey, val);
    return this;
  }
  
  public ProvFileStateParamBuilder withAppExpansion() {
    expansions.add(ProvQuery.FileExpansions.APP);
    return this;
  }
  
  public ProvFileStateParamBuilder withAppExpansionCurrentState(Provenance.AppState currentAppState) {
    withAppExpansion();
    ParamBuilder.addToFilters(appStateFilter,
      Pair.with(ProvQuery.ExpansionApp.APP_STATE, currentAppState.name()));
    return this;
  }
  
  public ProvFileStateParamBuilder withAppExpansion(String appId) {
    withAppExpansion();
    ParamBuilder.addToFilters(appStateFilter,
      Pair.with(ProvQuery.ExpansionApp.APP_ID, appId));
    return this;
  }
  
  public ProvFileStateParamBuilder sortBy(String field, SortOrder order) {
    try {
      ProvQuery.Field sortField = ProvQuery.extractBaseField(field, ProvQuery.QueryType.QUERY_FILE_STATE);
      fileStateSortBy.add(Pair.with(sortField, order));
    } catch(GenericException ex) {
      String xattrKey = ProvQuery.processXAttrKeyAsKeyword(field);
      xAttrSortBy.add(Pair.with(xattrKey, order));
    }
    return this;
  }
  
  public boolean hasAppExpansion() {
    return expansions.contains(ProvQuery.FileExpansions.APP);
  }
}
