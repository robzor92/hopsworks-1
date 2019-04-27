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

import io.hops.hopsworks.common.provenance.ProvFileQueryParams;
import io.hops.hopsworks.exceptions.GenericException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;

public class ProvFileQueryParamsBean {
  @QueryParam("inodeId")
  private Long inodeId;
  
  @QueryParam("withAppState") 
  private boolean withAppState;
  
  public ProvFileQueryParamsBean(
    @QueryParam("inodeId") Long inodeId,
    @QueryParam("withAppState") @DefaultValue("false") boolean withAppState) {
    this.inodeId = inodeId;
    this.withAppState = withAppState;
  }
  
  public ProvFileQueryParamsBean() {
  }

  public Long getInodeId() {
    return inodeId;
  }

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  public boolean isWithAppState() {
    return withAppState;
  }

  public void setWithAppState(boolean withAppState) {
    this.withAppState = withAppState;
  }
  
  public ProvFileQueryParams params(Integer projectId) throws GenericException {
    return ProvFileQueryParams.instance(projectId, inodeId, withAppState);
  }

  @Override
  public String toString() {
    return "ProvFileQueryParamsBean{"
      + (inodeId == null ? "" : " inodeId=" + inodeId)
      + " withAppState=" + withAppState 
      + '}';
  }
}
