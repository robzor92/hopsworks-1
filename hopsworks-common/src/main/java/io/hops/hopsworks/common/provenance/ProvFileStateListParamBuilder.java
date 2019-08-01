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

import java.util.Map;

public class ProvFileStateListParamBuilder {
  private Integer projectId;
  private String assetName;
  private String likeAssetName;
  private String userName;
  private String likeUserName;
  private Long createdBeforeTimestamp;
  private Long createdAfterTimestamp;
  private Map<String, String> xattrsExact;
  private Map<String, String> xattrsLike;
  private String appId;
  private String mlType;
  private Provenance.AppState currentAppState;
  private boolean withAppState;
  
  public ProvFileStateListParamBuilder() {}
  
  public Integer getProjectId() {
    return projectId;
  }
  
  public ProvFileStateListParamBuilder withProjectId(Integer projectId) {
    this.projectId = projectId;
    return this;
  }
  
  public String getAssetName() {
    return assetName;
  }
  
  public ProvFileStateListParamBuilder withAssetName(String assetName) {
    this.assetName = assetName;
    return this;
  }
  
  public String getLikeAssetName() {
    return likeAssetName;
  }
  
  public ProvFileStateListParamBuilder withLikeAssetName(String likeAssetName) {
    this.likeAssetName = likeAssetName;
    return this;
  }
  
  public String getUserName() {
    return userName;
  }
  
  public ProvFileStateListParamBuilder withUserName(String userName) {
    this.userName = userName;
    return this;
  }
  
  public String getLikeUserName() {
    return likeUserName;
  }
  
  public ProvFileStateListParamBuilder withLikeUserName(String likeUserName) {
    this.likeUserName = likeUserName;
    return this;
  }
  
  public Long getCreatedBeforeTimestamp() {
    return createdBeforeTimestamp;
  }
  
  public ProvFileStateListParamBuilder withCreatedBeforeTimestamp(Long createdBeforeTimestamp) {
    this.createdBeforeTimestamp = createdBeforeTimestamp;
    return this;
  }
  
  public Long getCreatedAfterTimestamp() {
    return createdAfterTimestamp;
  }
  
  public ProvFileStateListParamBuilder withCreatedAfterTimestamp(Long createdAfterTimestamp) {
    this.createdAfterTimestamp = createdAfterTimestamp;
    return this;
  }
  
  public Map<String, String> getXattrsExact() {
    return xattrsExact;
  }
  
  public ProvFileStateListParamBuilder withXattrsExact(Map<String, String> xattrsExact) {
    this.xattrsExact = xattrsExact;
    return this;
  }
  
  public Map<String, String> getXAttrsLike() {
    return xattrsLike;
  }
  
  public ProvFileStateListParamBuilder withXAttrsLike(Map<String, String> xattrsLike) {
    this.xattrsLike = xattrsLike;
    return this;
  }
  
  public String getAppId() {
    return appId;
  }
  
  public ProvFileStateListParamBuilder withAppId(String appId) {
    this.appId = appId;
    return this;
  }
  
  public String getMlType() {
    return mlType;
  }
  
  public ProvFileStateListParamBuilder withMlType(String mlType) {
    this.mlType = mlType;
    return this;
  }
  
  public boolean isWithAppState() {
    return withAppState;
  }
  
  public ProvFileStateListParamBuilder withAppState(boolean withAppState) {
    this.withAppState = withAppState;
    return this;
  }
  
  public Provenance.AppState getCurrentAppState() {
    return currentAppState;
  }
  
  public ProvFileStateListParamBuilder withCurrentAppState(Provenance.AppState currentAppState) {
    this.currentAppState = currentAppState;
    return this;
  }
  
  
  public ProvFileDetailsQueryParams fileDetails() throws GenericException {
    return ProvFileDetailsQueryParams.instance(projectId, assetName, likeAssetName, userName, likeUserName,
      createdBeforeTimestamp, createdAfterTimestamp, xattrsExact, xattrsLike, appId, withAppState);
  }
  
  public ProvMLAssetDetailsQueryParams mlAssetDetails() {
    return ProvMLAssetDetailsQueryParams.instance(mlType);
  }
  
  public ProvFileAppDetailsQueryParams appDetails() {
    return ProvFileAppDetailsQueryParams.instance(withAppState, currentAppState);
  }
  
}
