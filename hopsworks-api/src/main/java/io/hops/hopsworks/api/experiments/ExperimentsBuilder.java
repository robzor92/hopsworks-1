package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.common.provenance.FProvMLAssetHit;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.exceptions.ServiceException;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.UriInfo;
import java.util.Date;
import java.util.List;
import java.util.Map;
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

  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project, FProvMLAssetHit fileProvenanceHit) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .path(fileProvenanceHit.getMlId())
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

      GenericEntity<List<FProvMLAssetHit>> searchResults = new GenericEntity<List<FProvMLAssetHit>>(
          elasticController.fileProvenanceByMLType(Provenance.MLType.EXPERIMENT.name(), project.getId(), true)) {
      };

      searchResults.getEntity().forEach((fileProvenanceHit) ->
          dto.addItem(build(uriInfo, resourceRequest, project, fileProvenanceHit)));
    }

    return dto;
  }


  //Build specific
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project,
                             FProvMLAssetHit fileProvenanceHit) {

    ExperimentDTO experimentDTO = new ExperimentDTO();

    uri(experimentDTO, uriInfo, project, fileProvenanceHit);

    experimentDTO.setType(fileProvenanceHit.getMlType());
    experimentDTO.setId(fileProvenanceHit.getMlId());
    experimentDTO.setStarted(fileProvenanceHit.getCreateTimestamp());
    experimentDTO.setStarted(new Date().toString());

    if(fileProvenanceHit.getXattrs().containsKey("config")) {
      JSONObject config = new JSONObject(fileProvenanceHit.getXattrs().get("config"));
      experimentDTO.setName(config.get("name").toString());
      experimentDTO.setUserFullName(config.get("userFullName").toString());
    }



    LOGGER.log(Level.SEVERE, "xattrs " + fileProvenanceHit.getXattrs().size());
    for(Map.Entry<String, String> kv: fileProvenanceHit.getXattrs().entrySet()) {
      LOGGER.log(Level.SEVERE,kv.getKey() + "   "  + kv.getValue());
    }

    LOGGER.log(Level.SEVERE, "map? " + fileProvenanceHit.getMap().size());
    for(Map.Entry<String, String> kv: fileProvenanceHit.getMap().entrySet()) {
      LOGGER.log(Level.SEVERE,kv.getKey() + "   "  + kv.getValue());
    }

    expand(experimentDTO, resourceRequest);
    if (experimentDTO.isExpand()) {




      //dto.setId(job.getId());
    }
    return experimentDTO;
  }
}
