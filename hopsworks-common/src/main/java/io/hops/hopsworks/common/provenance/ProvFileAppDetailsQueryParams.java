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
package io.hops.hopsworks.common.provenance;

public class ProvFileAppDetailsQueryParams {
  public final Provenance.AppState currentState;
  public final boolean withAppState;
  
  public ProvFileAppDetailsQueryParams(boolean withAppState, Provenance.AppState currentState) {
    this.withAppState = withAppState;
    this.currentState = currentState;
  }
  
  public static ProvFileAppDetailsQueryParams instance(boolean withAppState, Provenance.AppState currentState) {
    return new ProvFileAppDetailsQueryParams(withAppState, currentState);
  }
  
  public static ProvFileAppDetailsQueryParams noAppDetails() {
    return new ProvFileAppDetailsQueryParams(false, null);
  }
}
