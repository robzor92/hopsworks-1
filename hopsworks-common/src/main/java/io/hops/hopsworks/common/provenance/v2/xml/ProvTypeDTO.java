package io.hops.hopsworks.common.provenance.v2.xml;

import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.ws.rs.QueryParam;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;
import java.util.logging.Level;

@XmlRootElement
public class ProvTypeDTO {
  @QueryParam("metaStatus")
  private Inode.MetaStatus metaStatus;
  @QueryParam("provStatus")
  private OpStore provStatus;
  
  public ProvTypeDTO() {}
  
  private ProvTypeDTO(Inode.MetaStatus metaStatus, OpStore provStatus) {
    this.metaStatus = metaStatus;
    this.provStatus = provStatus;
  }
  
  public Inode.MetaStatus getMetaStatus() {
    return metaStatus;
  }
  
  public void setMetaStatus(Inode.MetaStatus metaStatus) {
    this.metaStatus = metaStatus;
  }
  
  public OpStore getProvStatus() {
    return provStatus;
  }
  
  public void setProvStatus(OpStore provStatus) {
    this.provStatus = provStatus;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProvTypeDTO that = (ProvTypeDTO) o;
    return metaStatus == that.metaStatus &&
      provStatus == that.provStatus;
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(metaStatus, provStatus);
  }
  
  public static ProvTypeDTO.ProvType provTypeFromString(String aux) throws GenericException {
    ProvType type;
    try {
      return ProvType.valueOf(aux.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "malformed type", "malformed type", e);
    }
  }
  
  public static ProvTypeDTO.ProvType getProvType(ProvTypeDTO aux) throws GenericException {
    switch(aux.metaStatus) {
      case DISABLED: return ProvType.DISABLED;
      case META_ENABLED: return ProvType.META;
      case MIN_PROV_ENABLED: return ProvType.MIN;
      case FULL_PROV_ENABLED: return ProvType.FULL;
      default: throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "malformed type dto");
    }
  }
  
  public enum OpStore {
    NONE,
    STATE,
    ALL
  }
  
  public enum ProvType {
    DISABLED(new ProvTypeDTO(Inode.MetaStatus.DISABLED, OpStore.NONE)),
    META(new ProvTypeDTO(Inode.MetaStatus.META_ENABLED, OpStore.NONE)),
    MIN(new ProvTypeDTO(Inode.MetaStatus.MIN_PROV_ENABLED, OpStore.STATE)),
    FULL(new ProvTypeDTO(Inode.MetaStatus.FULL_PROV_ENABLED, OpStore.ALL));
  
    public ProvTypeDTO dto;
    ProvType(ProvTypeDTO dto) {
      this.dto = dto;
    }
  }
}
