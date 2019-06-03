package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.experiments.dto.ExperimentConfiguration;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.UriInfo;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsBuilder {


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
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project) {
    ExperimentDTO dto = new ExperimentDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);

    if(dto.isExpand()) {
      /*
      AbstractFacade.CollectionInfo collectionInfo = experimentsFacade.findByProject(resourceRequest.getOffset(),
          resourceRequest.getLimit(),
          resourceRequest.getFilter(),
          resourceRequest.getSort(), project);
       */

      //set the count
      //dto.setCount(collectionInfo.getCount());
      //collectionInfo.getItems().forEach((job) -> dto.addItem(build(uriInfo, resourceRequest, (Jobs) job)));
    }


    return dto;
  }


  //Build specific
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest,
                             ExperimentConfiguration experimentConfiguration) {
    ExperimentDTO dto = new ExperimentDTO();
    //uri(dto, uriInfo, experimentConfiguration);
    expand(dto, resourceRequest);
    if (dto.isExpand()) {
      //dto.setId(job.getId());
    }
    return dto;
  }
}
