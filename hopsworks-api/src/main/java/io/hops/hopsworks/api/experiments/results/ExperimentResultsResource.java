package io.hops.hopsworks.api.experiments.results;

import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.experiments.ExperimentConfigurationConverter;
import io.hops.hopsworks.common.experiments.dto.results.ExperimentResultSummaryDTO;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
import io.swagger.annotations.ApiOperation;
import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.BeanParam;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.logging.Level;

@RequestScoped
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentResultsResource {

  private Project project;
  private String experimentId;

  @EJB
  private ExperimentResultsBuilder experimentResultsBuilder;
  @EJB
  private ProvenanceController provenanceController;
  @EJB
  private ExperimentConfigurationConverter experimentConfigurationConverter;

  public ExperimentResultsResource() {
  }

  public ExperimentResultsResource setProject(Project project, String experimentId) {
    this.project = project;
    this.experimentId = experimentId;
    return this;
  }

  public Project getProject() {
    return project;
  }

  @ApiOperation(value = "Get results information", response = ExperimentResultSummaryDTO.class)
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  @JWTRequired(acceptedTokens={Audience.API}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response getResults(@Context UriInfo uriInfo,
                             @BeanParam Pagination pagination,
                             @BeanParam ExperimentResultsBeanParam experimentResultsBeanParam)
      throws ExperimentsException {
    ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.RESULTS);
    resourceRequest.setOffset(pagination.getOffset());
    resourceRequest.setLimit(pagination.getLimit());
    resourceRequest.setSort(experimentResultsBeanParam.getSortBySet());

    ExperimentResultSummaryDTO dto = experimentResultsBuilder.build(uriInfo, resourceRequest, project, experimentId);
    if(dto == null) {
      throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.RESULTS_NOT_FOUND, Level.FINE);
    }
    return Response.ok().entity(dto).build();
  }
}
