package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.AbstractFacade;
import io.hops.hopsworks.common.dao.jobs.description.Jobs;
import io.hops.hopsworks.common.dao.project.Project;

import javax.ws.rs.core.UriInfo;

public class ExperimentsBuilder {

  public ExperimentsDTO uri(ExperimentsDTO dto, UriInfo uriInfo, Project project) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .build());
    return dto;
  }

  public ExperimentsDTO expand(ExperimentsDTO dto, ResourceRequest resourceRequest) {
    if (resourceRequest != null && resourceRequest.contains(ResourceRequest.Name.EXPERIMENTS)) {
      dto.setExpand(true);
    }
    return dto;
  }

  //Build collection
  public ExperimentsDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project) {
    ExperimentsDTO dto = new ExperimentsDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);
    if(dto.isExpand()) {
      AbstractFacade.CollectionInfo collectionInfo = experimentsFacade.findByProject(resourceRequest.getOffset(),
          resourceRequest.getLimit(),
          resourceRequest.getFilter(),
          resourceRequest.getSort(), project);
      //set the count
      dto.setCount(collectionInfo.getCount());
      collectionInfo.getItems().forEach((job) -> dto.addItem(build(uriInfo, resourceRequest, (Jobs) job)));
    }
    return dto;
  }

  //Build specific
  public ExperimentsDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Jobs job) {
    ExperimentsDTO dto = new ExperimentsDTO();
    uri(dto, uriInfo, job);
    expand(dto, resourceRequest);
    if (dto.isExpand()) {
      dto.setId(job.getId());
    }
    return dto;
  }
}
