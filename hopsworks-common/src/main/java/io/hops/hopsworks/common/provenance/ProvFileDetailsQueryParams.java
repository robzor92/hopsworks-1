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

import java.util.HashMap;
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
  public final Map<String, String> xattrsExact;
  public final Map<String, String> xattrsLike;
  public final String appId;
  public final boolean withFullPath;

  private ProvFileDetailsQueryParams(Integer projectId,
    String assetName, String likeAssetName,
    String userName, String likeUserName, 
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    Map<String, String> xattrsExact, Map<String, String> xattrsLike,
    String appId, boolean withFullPath) {
    this.projectId = projectId;
    this.assetName = assetName;
    this.likeAssetName = likeAssetName;
    this.userName = userName;
    this.likeUserName = likeUserName;
    this.createdBeforeTimestamp = createdBeforeTimestamp;
    this.createdAfterTimestamp = createdAfterTimestamp;
    this.xattrsExact = xattrsExact;
    this.xattrsLike = xattrsLike;
    this.appId = appId;
    this.withFullPath = withFullPath;
  }

  public static ProvFileDetailsQueryParams instance(Integer projectId,
    String assetName, String likeAssetName,
    String userName, String likeUserName, 
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    String xattrsExact,  String xattrsLike,
    String appId, boolean withFullPath) throws GenericException {
    Map<String, String> xattrsExactMap = provInternalXAttrsMap(xattrsExact);
    Map<String, String> xattrsLikeMap = provInternalXAttrsMap(xattrsLike);
    checkParams(projectId, assetName, likeAssetName, userName, likeUserName,
      createdBeforeTimestamp, createdAfterTimestamp, xattrsExactMap, xattrsLikeMap, appId, withFullPath);
    return new ProvFileDetailsQueryParams(projectId, assetName, likeAssetName, userName, likeUserName,
      createdBeforeTimestamp, createdAfterTimestamp, xattrsExactMap, xattrsLikeMap, appId, withFullPath);
  }
  
  public static ProvFileDetailsQueryParams instance(Integer projectId,
    String assetName, String likeAssetName,
    String userName, String likeUserName,
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    Map<String, String> xattrsExact,  Map<String, String> xattrsLike,
    String appId, boolean withFullPath) throws GenericException{
    
    Map<String, String> xattrsExactMap = provInternalXAttrsMap(xattrsExact);
    Map<String, String> xattrsLikeMap = provInternalXAttrsMap(xattrsLike);
    checkParams(projectId, assetName, likeAssetName, userName, likeUserName,
      createdBeforeTimestamp, createdAfterTimestamp, xattrsExact, xattrsLike, appId, withFullPath);
    return new ProvFileDetailsQueryParams(projectId, assetName, likeAssetName, userName, likeUserName,
      createdBeforeTimestamp, createdAfterTimestamp, xattrsExactMap, xattrsLikeMap, appId, withFullPath);
  }
  
  private static void checkParams(Integer projectId,
    String assetName, String likeAssetName,
    String userName, String likeUserName,
    Long createdBeforeTimestamp, Long createdAfterTimestamp,
    Map<String, String> xattrsExact,  Map<String, String> xattrsLike,
    String appId, boolean withFullPath) throws GenericException {
    if (assetName != null && likeAssetName != null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - set only one - either like or exact - assetName");
    }
    if (userName != null && likeUserName != null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "provenance query - set only one - either like or exact - userName");
    }
  }
  
  private static Map<String, String> provInternalXAttrsMap(Map<String, String> xattrs) {
    Map<String, String> result = new HashMap<>();
    if (xattrs == null || xattrs.isEmpty()) {
      return result;
    }
    for(Map.Entry<String, String> e : xattrs.entrySet()) {
      String adjustedKey = adjustXAttrKey(e.getKey());
      result.put(adjustedKey, e.getValue());
    }
    return result;
  }
  
  private static Map<String, String> provInternalXAttrsMap(String xattrs) throws GenericException {
    Map<String, String> result = new TreeMap<String, String>();
    if (xattrs == null || xattrs.isEmpty()) {
      return result;
    }
    String[] params = xattrs.split(",");
    
    for (String p : params) {
      String[] aux = p.split(":");
      if(aux.length != 2 || aux[0].isEmpty()) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
          "malformed xattrsExact:" + xattrs);
      }
      String adjustedKey = adjustXAttrKey(aux[0]);
      result.put(adjustedKey, aux[1]);
    }
    return result;
  }
  
  private static String adjustXAttrKey(String key) {
    String keyParts[] =key.split("\\.");
    StringJoiner keyj = new StringJoiner(".");
    if(keyParts.length == 1) {
      keyj.add(keyParts[0]).add("raw");
    } else {
      keyj.add(keyParts[0]).add("value");
      for(int i = 1; i < keyParts.length; i++) keyj.add(keyParts[i]);
    }
    return keyj.toString();
  }
}
