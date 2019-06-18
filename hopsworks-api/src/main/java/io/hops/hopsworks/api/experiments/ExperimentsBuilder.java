package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.provenance.ProvenanceService;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.common.provenance.FileProvenanceHit;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.exceptions.ServiceException;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsBuilder {

  private static final Logger LOGGER = Logger.getLogger(ExperimentsResource.class.getName());

  @EJB
  private ElasticController elasticController;


  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .build());
    return dto;
  }

  public ExperimentDTO expand(ExperimentDTO dto, ResourceRequest resourceRequest) {
    if (resourceRequest != null && resourceRequest.contains(ResourceRequest.Name.EXPERIMENTS)) {
      dto.setExpand(true);
    }
    return dto;
  }


  //Build collection
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project)
      throws ServiceException, ProjectException {
    ExperimentDTO dto = new ExperimentDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);

    if(dto.isExpand()) {

      GenericEntity<List<FileProvenanceHit>> searchResults = new GenericEntity<List<FileProvenanceHit>>(
          elasticController.fileProvenanceByMLType(ProvenanceService.MLType.EXPERIMENT.name(), project.getId())) {
      };

      searchResults.getEntity().forEach((experiment) ->
          dto.addItem(build(uriInfo, resourceRequest, experiment)));
    }


    return dto;
  }


  //Build specific
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest,
                             FileProvenanceHit fileProvenanceHit) {

    LOGGER.log(Level.SEVERE, fileProvenanceHit.getMlType());
    LOGGER.log(Level.SEVERE, fileProvenanceHit.toString());

    ExperimentDTO dto = new ExperimentDTO();
    //uri(dto, uriInfo, experimentConfiguration);
    expand(dto, resourceRequest);
    if (dto.isExpand()) {

      //dto.setId(job.getId());
    }
    return dto;
  }
}
