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
import io.hops.hopsworks.exceptions.GenericException;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProvFileOpsParamBuilder {
  private Map<String, List<Pair<ProvElastic.FileOpsFilter, Object>>> fileOpsFilter = new HashMap<>();
  
  public ProvFileOpsParamBuilder withQueryParamFileOps(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<ProvElastic.FileOpsFilter, Object> p = ProvElastic.extractFileOpsParam(param);
      addToFileOpsFilters(p.getValue0(), p.getValue1());
    }
    return this;
  }
  
  public Map<String, List<Pair<ProvElastic.FileOpsFilter, Object>>> getFileOpsFilter() {
    return fileOpsFilter;
  }
  
  public ProvFileOpsParamBuilder withProjectInodeId(Long projectInodeId) {
    addToFileOpsFilters(ProvElastic.FileOpsFilter.PROJECT_I_ID, projectInodeId);
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileInodeId(Long fileInodeId) {
    addToFileOpsFilters(ProvElastic.FileOpsFilter.FILE_INODE_ID, fileInodeId);
    return this;
  }
  
  
  public ProvFileOpsParamBuilder withAppId(String appId) {
    addToFileOpsFilters(ProvElastic.FileOpsFilter.APP_ID, appId);
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileOperation(ProvFileOps fileOp) {
    addToFileOpsFilters(ProvElastic.FileOpsFilter.FILE_OPERATION, fileOp.name());
    return this;
  }
  
  private void addToFileOpsFilters(ProvElastic.FileOpsFilter field, Object val) {
    List<Pair<ProvElastic.FileOpsFilter, Object>> fieldFilters = fileOpsFilter.get(field.queryParamName);
    if(fieldFilters == null) {
      fieldFilters = new LinkedList<>();
      fileOpsFilter.put(field.queryParamName, fieldFilters);
    }
    fieldFilters.add(Pair.with(field, val));
  }
}
