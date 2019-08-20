package io.hops.hopsworks.api.experiments.results;

import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.experiments.ExperimentConfigurationConverter;
import io.hops.hopsworks.common.experiments.dto.ExperimentDescription;
import io.hops.hopsworks.common.experiments.dto.results.ExperimentResultSummaryDTO;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
import io.swagger.annotations.ApiOperation;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.BeanParam;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.Collection;
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

  public ExperimentResultsResource(){
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
                             @BeanParam Pagination pagination)
      throws ExperimentsException, GenericException, ServiceException {
    ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.RESULTS);
    resourceRequest.setOffset(pagination.getOffset());
    resourceRequest.setLimit(pagination.getLimit());

    ProvFileStateParamBuilder provFilesParamBuilder = new ProvFileStateParamBuilder()
        .withProjectInodeId(project.getInode().getId())
        .withMlType(Provenance.MLType.EXPERIMENT.name())
        .withAppState()
        .withMlId(this.experimentId);

    GenericEntity<Collection<FileState>> fileProvenanceHits = new GenericEntity<Collection<FileState>>(
        provenanceController.provFileStateList(provFilesParamBuilder).values()) {
    };

    if(fileProvenanceHits.getEntity().isEmpty()) {
      throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.EXPERIMENT_NOT_FOUND, Level.FINE);
    }

    FileState experiment = fileProvenanceHits.getEntity().iterator().next();

    JSONObject config = new JSONObject(experiment.getXattrs().get("config"));

    ExperimentDescription experimentDescription =
        experimentConfigurationConverter.unmarshalDescription(config.toString());

    ExperimentResultSummaryDTO dto = experimentResultsBuilder.build(uriInfo, resourceRequest, project, experimentId,
        experimentDescription.getOptimizationKey(), experimentDescription.getDirection());
    if(dto == null) {
      throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.RESULTS_NOT_FOUND, Level.FINE);
    }
    return Response.ok().entity(dto).build();
  }
}
