package io.hops.hopsworks.api.experiments.provenance;

import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.experiments.dto.provenance.ExperimentProvenanceDTO;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
import io.swagger.annotations.ApiOperation;

import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.util.logging.Level;

@RequestScoped
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentFileProvenanceResource {

  private Project project;
  private String experimentId;

  @EJB
  private ExperimentFileProvenanceBuilder experimentFileProvenanceBuilder;

  public ExperimentFileProvenanceResource(){
  }

  public ExperimentFileProvenanceResource setProject(Project project, String experimentId) {
    this.project = project;
    this.experimentId = experimentId;
    return this;
  }

  public Project getProject() {
    return project;
  }

  @ApiOperation(value = "Get provenance information", response = ExperimentProvenanceDTO.class)
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  @JWTRequired(acceptedTokens={Audience.API}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response getProvenance(@Context SecurityContext sc,
                                 @Context UriInfo uriInfo) throws ExperimentsException {
    ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.PROVENANCE);
    ExperimentProvenanceDTO dto = experimentFileProvenanceBuilder.build(uriInfo, resourceRequest,
        project, this.experimentId);
    if(dto == null) {
      throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.PROVENANCE_NOT_FOUND, Level.FINE);
    }
    return Response.ok().entity(dto).build();
  }
}
