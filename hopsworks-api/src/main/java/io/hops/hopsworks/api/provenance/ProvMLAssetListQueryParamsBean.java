package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.common.provenance.ProvMLAssetDetailsQueryParams;

import javax.ws.rs.QueryParam;

public class ProvMLAssetListQueryParamsBean {
  @QueryParam("mlType")
  private String mlType;
  
  public ProvMLAssetListQueryParamsBean() {}
  
  public ProvMLAssetListQueryParamsBean(
    @QueryParam("mlType") String mlType) {
    this.mlType = mlType;
  }
  
  public String getMlType() { return mlType; }
  
  public void setMlType(String mlType) { this.mlType = mlType; }
  
  @Override
  public String toString() {
    return "ProvMLAssetListQueryParamsBean{"
      + (mlType == null ? "" : " mlType=" + mlType)
      + '}';
  }
  
  public ProvMLAssetDetailsQueryParams params() {
    return ProvMLAssetDetailsQueryParams.instance(mlType);
  }
}
