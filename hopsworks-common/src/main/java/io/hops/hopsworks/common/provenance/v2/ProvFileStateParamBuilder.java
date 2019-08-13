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

import io.hops.hopsworks.common.provenance.ProvElastic;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static io.hops.hopsworks.common.provenance.ProvElastic.extractAppStateParam;

public class ProvFileStateParamBuilder {
  private Map<String, Pair<ProvElastic.FileStateFilter, Object>> fileStateFilter = new HashMap<>();
  private Map<String, String> exactXAttrFilter = new HashMap<>();
  private Map<String, String> likeXAttrFilter = new HashMap<>();
  private Set<ProvElastic.FileStateExpansions> expansions = new HashSet<>();
  private Map<String, List<Pair<ProvElastic.AppStateFilter, Object>>> appStateFilter = new HashMap<>();
  
  public ProvFileStateParamBuilder withQueryParamFileState(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<ProvElastic.FileStateFilter, Object> p = ProvElastic.extractFileStateParam(param);
      fileStateFilter.put(p.getValue0().queryParamName, p);
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamExactXAttr(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> p = ProvElastic.extractXAttrParam(param);
      exactXAttrFilter.put(p.getValue0(), p.getValue1());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamLikeXAttr(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> p = ProvElastic.extractXAttrParam(param);
      likeXAttrFilter.put(p.getValue0(), p.getValue1());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamExpansions(Set<String> params) throws GenericException {
    for(String param : params) {
      try {
        expansions.add(ProvElastic.FileStateExpansions.valueOf(param));
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "param " + param + " not supported - supported params:"
            + EnumSet.allOf(ProvElastic.FileStateExpansions.class),
          "exception extracting FilterBy param", e);
      }
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamAppState(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<ProvElastic.AppStateFilter, Object> p = extractAppStateParam(param);
      List<Pair<ProvElastic.AppStateFilter, Object>> fieldFilters =
        appStateFilter.get(p.getValue0().paramName());
      if(fieldFilters == null) {
        fieldFilters = new LinkedList<>();
        appStateFilter.put(p.getValue0().paramName(), fieldFilters);
      }
      fieldFilters.add(p);
    }
    return this;
  }
  
  public Collection<Pair<ProvElastic.FileStateFilter, Object>> getFileStateFilter() {
    return fileStateFilter.values();
  }
  
  public Map<String, String> getExactXAttrFilter() {
    return exactXAttrFilter;
  }
  
  public Map<String, String> getLikeXAttrFilter() {
    return likeXAttrFilter;
  }
  
  public Set<ProvElastic.FileStateExpansions> getExpansions() {
    return expansions;
  }
  
  public Map<String, List<Pair<ProvElastic.AppStateFilter, Object>>> getAppStateFilter() {
    return appStateFilter;
  }
  
  public ProvFileStateParamBuilder withProjectInodeId(Long projectInodeId) {
    fileStateFilter.put(ProvElastic.FileStateFilter.PROJECT_I_ID.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.PROJECT_I_ID, projectInodeId));
    return this;
  }
  
  public ProvFileStateParamBuilder withInodeId(Long inodeId) {
    fileStateFilter.put(ProvElastic.FileStateFilter.INODE_ID.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.INODE_ID, inodeId));
    return this;
  }
  
  public ProvFileStateParamBuilder withMlId(String mlId) {
    fileStateFilter.put(ProvElastic.FileStateFilter.ML_ID.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.ML_ID, mlId));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileName(String fileName) {
    fileStateFilter.put(ProvElastic.FileStateFilter.FILE_NAME.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.FILE_NAME, fileName));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileNameLike(String fileName) {
    fileStateFilter.put(ProvElastic.FileStateFilter.FILE_NAME_LIKE.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.FILE_NAME_LIKE, fileName));
    return this;
  }
  
  public ProvFileStateParamBuilder withUserId(String userId) {
    fileStateFilter.put(ProvElastic.FileStateFilter.USER_ID.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.USER_ID, userId));
    return this;
  }
  
  public ProvFileStateParamBuilder createdBefore(Long timestamp) {
    fileStateFilter.put(ProvElastic.FileStateFilter.CREATE_TIMESTAMP_LT.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.CREATE_TIMESTAMP_LT, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder createdAfter(Long timestamp) {
    fileStateFilter.put(ProvElastic.FileStateFilter.CREATE_TIMESTAMP_GT.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.CREATE_TIMESTAMP_GT, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder withAppId(String appId) {
    fileStateFilter.put(ProvElastic.FileStateFilter.APP_ID.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.APP_ID, appId));
    return this;
  }
  
  public ProvFileStateParamBuilder withMlType(String mlType) {
    fileStateFilter.put(ProvElastic.FileStateFilter.ML_TYPE.queryParamName,
      Pair.with(ProvElastic.FileStateFilter.ML_TYPE, mlType));
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrs(Map<String, String> xAttrs) {
    for(Map.Entry<String, String> xAttr : xAttrs.entrySet()) {
      withXAttr(xAttr.getKey(), xAttr.getValue());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttr(String key, String val) {
    Pair<String, String> x = ProvElastic.processXAttr(key, val);
    exactXAttrFilter.put(x.getValue0(), x.getValue1());
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrsLike(Map<String, String> xAttrs) {
    for(Map.Entry<String, String> xAttr : xAttrs.entrySet()) {
      withXAttrLike(xAttr.getKey(), xAttr.getValue());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrLike(String key, String val) {
    Pair<String, String> x = ProvElastic.processXAttr(key, val);
    likeXAttrFilter.put(x.getValue0(), x.getValue1());
    return this;
  }
  
  public ProvFileStateParamBuilder withAppState() {
    expansions.add(ProvElastic.FileStateExpansions.APP_STATE);
    return this;
  }
  
  public ProvFileStateParamBuilder withCurrentAppState(Provenance.AppState currentAppState) {
    withAppState();
    List<Pair<ProvElastic.AppStateFilter, Object>> fieldFilters =
      appStateFilter.get(ProvElastic.AppStateFilter.APP_STATE.queryParamName);
    if(fieldFilters == null) {
      fieldFilters = new LinkedList<>();
      appStateFilter.put(ProvElastic.AppStateFilter.APP_STATE.queryParamName, fieldFilters);
    }
    fieldFilters.add(Pair.with(ProvElastic.AppStateFilter.APP_STATE, currentAppState));
    return this;
  }
  
  public ProvFileStateParamBuilder withAppStateAppId(String appId) {
    List<Pair<ProvElastic.AppStateFilter, Object>> fieldFilters =
      appStateFilter.get(ProvElastic.AppStateFilter.APP_ID.queryParamName);
    if(fieldFilters == null) {
      fieldFilters = new LinkedList<>();
      appStateFilter.put(ProvElastic.AppStateFilter.APP_ID.queryParamName, fieldFilters);
    }
    fieldFilters.add(Pair.with(ProvElastic.AppStateFilter.APP_ID, appId));
    return this;
  }
  
  public boolean isWithAppState() {
    if(expansions.size() == 1) {
      return true;
    }
    return false;
  }
}
