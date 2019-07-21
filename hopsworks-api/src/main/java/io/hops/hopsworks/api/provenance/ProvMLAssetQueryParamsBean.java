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
package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.common.provenance.ProvMLAssetQueryParams;
import javax.ws.rs.QueryParam;

public class ProvMLAssetQueryParamsBean {
  @QueryParam("mlType")
  private String mlType;
  @QueryParam("mlId")
  private String mlId;
  
  public ProvMLAssetQueryParamsBean() {}
  
  public ProvMLAssetQueryParamsBean(
    @QueryParam("mlType") String mlType,
    @QueryParam("mlId") String mlId) {
    this.mlType = mlType;
    this.mlId = mlId;
  }
  
  public String getMlType() { return mlType; }
  
  public void setMlType(String mlType) { this.mlType = mlType; }
  
  public String getMlId() {
    return mlId;
  }
  
  public void setMlId(String mlId) {
    this.mlId = mlId;
  }
  
  @Override
  public String toString() {
    return "ProvMLAssetQueryParams{"
      + (mlType == null ? "" : " mlType=" + mlType)
      + (mlId == null ? "" : " mlId=" + mlId)
      + '}';
  }
  
  public ProvMLAssetQueryParams params() {
    return ProvMLAssetQueryParams.instance(mlType, mlId);
  }
}
