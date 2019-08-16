package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.api.filter.NoCacheResponse;
import io.hops.hopsworks.common.provenance.ProvFileOpHit;
import io.hops.hopsworks.common.provenance.ProvFileOpsCompactByApp;
import io.hops.hopsworks.common.provenance.ProvFileOpsSummaryByApp;
import io.hops.hopsworks.common.provenance.ProvFileStateHit;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.SimpleResult;
import io.hops.hopsworks.common.provenance.v2.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class ProvenanceResourceHelper {
  public static Response getFileStates(NoCacheResponse noCacheResponse, ProvenanceController provenanceCtrl,
    ProvFileStateParamBuilder params, ProjectProvenanceResource.FileStructReturnType returnType)
    throws GenericException, ServiceException {
    switch(returnType) {
      case LIST:
        Collection<ProvFileStateHit> listResult = provenanceCtrl.provFileState(params).values();
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
          .entity(new GenericEntity<Collection<ProvFileStateHit>>(listResult) {}).build();
      case TREE:
        Map<Long, ProvenanceController.StructNode> treeResult = provenanceCtrl.provFileStateTree(params);
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
          .entity(new GenericEntity<Map<Long, ProvenanceController.StructNode>>(treeResult) {}).build();
      case COUNT:
        Long countResult = provenanceCtrl.provFileStateCount(params);
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
          .entity(new SimpleResult<>(countResult)).build();
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
          "return type: " + returnType + " is not managed");
    }
  }
  
  public static Response getFileOps(NoCacheResponse noCacheResponse, ProvenanceController provenanceCtrl,
    ProvFileOpsParamBuilder params, ProjectProvenanceResource.FileOpsCompactionType opsCompaction,
    ProjectProvenanceResource.FileStructReturnType returnType)
    throws ServiceException, GenericException {
    if(ProjectProvenanceResource.FileStructReturnType.COUNT.equals(returnType)) {
      Long result = provenanceCtrl.provFileOpsCount(params);
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
        .entity(new SimpleResult<>(result)).build();
    } else {
      List<ProvFileOpHit> result = provenanceCtrl.provFileOps(params);
      switch(opsCompaction) {
        case NONE:
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpHit>>(result) {}).build();
        case COMPACT:
          List<ProvFileOpsCompactByApp> compactResults = ProvFileOpsCompactByApp.compact(result);
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpsCompactByApp>>(compactResults) {}).build();
        case SUMMARY:
          List<ProvFileOpsSummaryByApp> summaryResults = ProvFileOpsSummaryByApp.summary(result);
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpsSummaryByApp>>(summaryResults) {}).build();
        default:
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
            "footprint filterType: " + returnType);
      }
    }
  }
}
