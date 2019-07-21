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

import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.logging.Level;

public class ProvFileDetailsQueryParams {
  public final Integer projectId;
  public final String assetName;
  public final String likeAssetName;
  public final String userName;
  public final String likeUserName;
  public final Long createdBeforeTimestamp;
  public final Long createdAfterTimestamp;
  public final Map<String, String> xattrs;
  public final String appId;

  private ProvFileDetailsQueryParams(Integer projectId,
    String assetName, String likeAssetName,
    String userName, String likeUserName, 
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    Map<String, String> xattrs,
    String appId) {
    this.projectId = projectId;
    this.assetName = assetName;
    this.likeAssetName = likeAssetName;
    this.userName = userName;
    this.likeUserName = likeUserName;
    this.createdBeforeTimestamp = createdBeforeTimestamp;
    this.createdAfterTimestamp = createdAfterTimestamp;
    this.xattrs = xattrs;
    this.appId = appId;
  }

  public static ProvFileDetailsQueryParams instance(Integer projectId,
    String assetName, String likeAssetName,
    String userName, String likeUserName, 
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    Map<String, String> xattrs, String appId) throws GenericException {
    if (assetName != null && likeAssetName != null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - set only one - either like or exact - assetName");
    }
    if (userName != null && likeUserName != null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - set only one - either like or exact - userName");
    }
    return new ProvFileDetailsQueryParams(projectId, assetName, likeAssetName, userName, likeUserName,
      createdBeforeTimestamp, createdAfterTimestamp, xattrs, appId);
  }
  
  public static ProvFileDetailsQueryParams projectMLAssets(Integer projectId)
    throws GenericException {
    return instance(projectId, null, null, null, null,
      null, null, null, null);
  }
  
  public static Map<String, String> getXAttrsMap(String xattrs) throws GenericException {
    Map<String, String> result = new TreeMap<String, String>();
    if (xattrs == null || xattrs.isEmpty()) {
      return result;
    }
    String[] params = xattrs.split(",");
    
    for (String p : params) {
      String[] aux = p.split(":");
      if(aux.length != 2 || aux[0].isEmpty()) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
          "malformed xattrs:" + xattrs);
      }
      String keyParts[] = aux[0].split("\\.");
      StringJoiner keyj = new StringJoiner(".");
      if(keyParts.length == 1) {
        keyj.add(keyParts[0]).add("raw");
      } else {
        keyj.add(keyParts[0]).add("value");
        for(int i = 1; i < keyParts.length; i++) keyj.add(keyParts[i]);
      }
      result.put(keyj.toString(), aux[1]);
    }
    return result;
  }
}
