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

import io.hops.hopsworks.common.provenance.ProvFileAppDetailsQueryParams;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.exceptions.GenericException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;

public class ProvFileAppDetailsQueryParamsBean {
  @DefaultValue("false")
  @QueryParam("withAppState")
  private boolean withAppState;
  
  @QueryParam("currentAppState")
  private Provenance.AppState currentState;
  
  public ProvFileAppDetailsQueryParamsBean(
    @QueryParam("withAppState") @DefaultValue("false") boolean withAppState,
    @QueryParam("currentAppState") Provenance.AppState currentState) {
    this.withAppState = withAppState;
    this.currentState = currentState;
  }
  
  public ProvFileAppDetailsQueryParamsBean() {}
  
  public boolean isWithAppState() {
    return withAppState;
  }
  
  public void setWithAppState(boolean withAppState) {
    this.withAppState = withAppState;
  }
  
  public Provenance.AppState getCurrentState() {
    return currentState;
  }
  
  public void setCurrentState(Provenance.AppState currentState) {
    this.currentState = currentState;
  }
  
  @Override
  public String toString() {
    return "ProvFileAppDetailsQueryParamsBean{"
      + " withAppState=" + withAppState
      + (currentState == null ? "" : " currentAppState=" + currentState)
      + '}';
  }
  
  public ProvFileAppDetailsQueryParams params() throws GenericException {
    return ProvFileAppDetailsQueryParams.instance(withAppState, currentState);
  }
}
