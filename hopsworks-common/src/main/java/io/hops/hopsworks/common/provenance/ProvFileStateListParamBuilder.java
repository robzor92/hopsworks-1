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
  private Long inodeId;
  private String mlId;
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
  
  public ProvFileStateListParamBuilder(Integer projectId, Long inodeId, String mlId, String mlType,
    boolean withAppState, Provenance.AppState currentAppState) {
    this.projectId = projectId;
    this.inodeId = inodeId;
    this.mlId = mlId;
    this.mlType = mlType;
    this.withAppState = withAppState;
    this.currentAppState = currentAppState;
  }
  
  public Integer getProjectId() {
    return projectId;
  }
  
  public ProvFileStateListParamBuilder withProjectId(Integer projectId) {
    this.projectId = projectId;
    return this;
  }
  
  public Long getInodeId() {
    return inodeId;
  }
  
  public void withInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  public String getMlId() {
    return mlId;
  }
  
  public void withMlId(String mlId) {
    this.mlId = mlId;
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
  
  public ProvFileQueryParams fileParams() throws GenericException {
    return ProvFileQueryParams.instance(projectId, inodeId, withAppState);
  }
  
  public ProvMLAssetQueryParams mlAssetParams() {
    return ProvMLAssetQueryParams.instance(mlType, mlId);
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
