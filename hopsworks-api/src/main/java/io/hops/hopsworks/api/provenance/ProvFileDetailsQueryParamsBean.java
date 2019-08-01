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

import io.hops.hopsworks.common.provenance.ProvFileDetailsQueryParams;
import io.hops.hopsworks.exceptions.GenericException;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;
import java.util.Map;

public class ProvFileDetailsQueryParamsBean {
  @QueryParam("fileName")
  private String assetName;
  
  @QueryParam("likeFileName")
  private String likeAssetName;
   
  @QueryParam("userName")
  private String userName;
  
  @QueryParam("likeUserName")
  private String likeUserName;
  
  @QueryParam("createdBefore")
  private Long createdBeforeTimestamp;
  
  @QueryParam("createdAfter")
  private Long createdAfterTimestamp;
  
  @QueryParam("xattrsExact")
  @ApiParam(value = "ex. key1:val1,key2:val2")
  private String xattrsExact;
  
  @QueryParam("xattrsLike")
  @ApiParam(value = "ex. key1:val1,key2:val2")
  private String xattrsLike;
  
  @QueryParam("appId")
  private String appId;
  
  @QueryParam("withFullPath")
  @DefaultValue("false")
  private boolean withFullPath;
  
  public ProvFileDetailsQueryParamsBean(
    @QueryParam("assetName") String assetName,
    @QueryParam("likeAssetName") String likeAssetName, 
    @QueryParam("userName") String userName,
    @QueryParam("likeUserName") String likeUserName,
    @QueryParam("createdBefore") long createdBeforeTimestamp, 
    @QueryParam("createdAfter") long createdAfterTimestamp,
    @QueryParam("xattrsExact") String xattrsExact,
    @QueryParam("xattrsLike") String xattrsLike,
    @QueryParam("appId") String appId,
    @QueryParam("withFullPath") @DefaultValue("false") boolean withFullPath) {
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
  
  public ProvFileDetailsQueryParamsBean() {}

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Long getCreatedBeforeTimestamp() {
    return createdBeforeTimestamp;
  }

  public void setCreatedBeforeTimestamp(Long createdBeforeTimestamp) {
    this.createdBeforeTimestamp = createdBeforeTimestamp;
  }

  public Long getCreatedAfterTimestamp() {
    return createdAfterTimestamp;
  }

  public void setCreatedAfterTimestamp(Long createdAfterTimestamp) {
    this.createdAfterTimestamp = createdAfterTimestamp;
  }

  public String getAssetName() {
    return assetName;
  }

  public void setAssetName(String assetName) {
    this.assetName = assetName;
  }

  public String getLikeAssetName() {
    return likeAssetName;
  }

  public void setLikeAssetName(String likeAssetName) {
    this.likeAssetName = likeAssetName;
  }

  public String getLikeUserName() {
    return likeUserName;
  }

  public void setLikeUserName(String likeUserName) {
    this.likeUserName = likeUserName;
  }

  public String getXattrsExact() {
    return xattrsExact;
  }

  public void setXattrsExact(String xattrsExact) throws GenericException {
    this.xattrsExact = xattrsExact;
  }
  
  public String getXattrsLike() {
    return xattrsLike;
  }
  
  public void setXattrsLike(String xattrsLike) {
    this.xattrsLike = xattrsLike;
  }
  
  public String getAppId() {
    return appId;
  }
  
  public void setAppId(String appId) {
    this.appId = appId;
  }
  
  public boolean isWithFullPath() {
    return withFullPath;
  }
  
  public void setWithFullPath(boolean withFullPath) {
    this.withFullPath = withFullPath;
  }
  
  @Override
  public String toString() {
    return "ProvFileDetailsQueryParamsBean{"
      + (assetName == null ? "" : " assetName=" + assetName)
      + (likeAssetName == null ? "" : " likeAssetName=" + likeAssetName)
      + (userName == null ? "" : " userName=" + userName)
      + (likeUserName == null ? "" : " likeUserName=" + likeUserName)
      + (createdBeforeTimestamp == null ? "" : " createdBeforeTimestamp=" + createdBeforeTimestamp)
      + (createdAfterTimestamp == null ? "" :" createdAfterTimestamp=" + createdAfterTimestamp)
      + (xattrsExact == null ? "" : " xattrsExact=" + xattrsExact)
      + (xattrsLike == null ? "" : " xattrsLike=" + xattrsLike)
      + (appId == null ? "" : " appId=" + appId)
      + " withFullPath=" + withFullPath
      + '}';
  }

  public ProvFileDetailsQueryParams params(Integer projectId) throws GenericException {
    Map<String, String> xattrsExactMap = ProvFileDetailsQueryParams.getXAttrsMap(xattrsExact);
    Map<String, String> xattrsLikeMap = ProvFileDetailsQueryParams.getXAttrsMap(xattrsLike);
    return ProvFileDetailsQueryParams.instance(projectId, assetName, likeAssetName,
      userName, likeUserName, createdBeforeTimestamp, createdAfterTimestamp, xattrsExactMap, xattrsLikeMap,
      appId, withFullPath);
  }
  
  public ProvFileDetailsQueryParams params() throws GenericException {
    Map<String, String> xattrsExactMap = ProvFileDetailsQueryParams.getXAttrsMap(xattrsExact);
    Map<String, String> xattrsLikeMap = ProvFileDetailsQueryParams.getXAttrsMap(xattrsLike);
    return ProvFileDetailsQueryParams.instance(null, assetName, likeAssetName,
      userName, likeUserName, createdBeforeTimestamp, createdAfterTimestamp, xattrsExactMap, xattrsLikeMap,
      appId, withFullPath);
  }
}
