package io.hops.hopsworks.api.experiments.provenance;

import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.experiments.dto.provenance.ExperimentProvenanceDTO;
import io.hops.hopsworks.common.provenance.ProvAppFootprintType;
import io.hops.hopsworks.common.provenance.ProvFileHit;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.exceptions.JobException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentProvenanceBuilder {

  @EJB
  private ProvenanceController provenanceController;

  public ExperimentProvenanceDTO uri(ExperimentProvenanceDTO dto, UriInfo uriInfo, Project project, String mlId) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .path(mlId)
        .path(ResourceRequest.Name.PROVENANCE.toString().toLowerCase())
        .build());
    return dto;
  }

  public ExperimentProvenanceDTO expand(ExperimentProvenanceDTO dto, ResourceRequest resourceRequest) {
    if (resourceRequest != null && resourceRequest.contains(ResourceRequest.Name.PROVENANCE)) {
      dto.setExpand(true);
    }
    return dto;
  }

  public ExperimentProvenanceDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project, String mlId)
      throws ExperimentsException {
    ExperimentProvenanceDTO dto = new ExperimentProvenanceDTO();
    uri(dto, uriInfo, project, mlId);
    expand(dto, resourceRequest);
    if (dto.isExpand()) {
      try {
        String appId = mlId.substring(0, mlId.lastIndexOf("_"));

        List<ProvFileHit> filesReadHits = provenanceController.provAppFootprint(project.getId(), appId,
            ProvAppFootprintType.INPUT);
        ArrayList<String> filesRead = new ArrayList<>();
        for(ProvFileHit file : filesReadHits) {
          filesRead.add(file.getInodeName());
        }
        dto.setFilesRead(filesRead.toArray(new String[0]));

        List<ProvFileHit> filesOutputHits = provenanceController.provAppFootprint(project.getId(), appId,
            ProvAppFootprintType.OUTPUT_ADDED);
        ArrayList<String> filesOutput = new ArrayList<>();
        for(ProvFileHit file : filesOutputHits) {
          filesOutput.add(file.getInodeName());
        }
        dto.setFilesOutput(filesOutput.toArray(new String[0]));

        List<ProvFileHit> filesDeletedHits = provenanceController.provAppFootprint(project.getId(), appId,
            ProvAppFootprintType.REMOVED);
        ArrayList<String> filesDeleted = new ArrayList<>();
        for(ProvFileHit file : filesDeletedHits) {
          filesDeleted.add(file.getInodeName());
        }
        dto.setFilesDeleted(filesDeleted.toArray(new String[0]));
      } catch(Exception e) {
        throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.PROVENANCE_FILE_QUERY_ERROR, Level.FINE,
            "Unable to get file provenance information for experiment", e.getMessage(), e);
      }
    }
    return dto;
  }
}