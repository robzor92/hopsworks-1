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

import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import java.util.logging.Level;

public class ProvFileQueryParams {

  public final Integer projectId;
  public final Long inodeId;
  public final boolean withAppState;

  private ProvFileQueryParams(Integer projectId, Long inodeId, boolean withAppState) {
    this.projectId = projectId;
    this.inodeId = inodeId;
    this.withAppState = withAppState;
  }
  
  @Override
  public String toString() {
    return "ProvFileQueryParams{"
      + " projectId=" + projectId
      + (inodeId == null ? "" : " inodeId=" + inodeId)
      + " withAppState=" + withAppState
      + '}';
  }

  public static ProvFileQueryParams instance(Integer projectId, Long inodeId, boolean withAppState)
    throws GenericException {
    if (projectId == null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "project provenance query - always confined to own project");
    }
    return new ProvFileQueryParams(projectId, inodeId, withAppState);
  }
}
