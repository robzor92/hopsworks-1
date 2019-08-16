package io.hops.hopsworks.api.experiments.results;

import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;

import io.hops.hopsworks.common.experiments.ExperimentConfigurationConverter;
import io.hops.hopsworks.common.experiments.dto.results.ExperimentResultSummaryDTO;
import io.hops.hopsworks.common.experiments.dto.results.ExperimentResultsDTO;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.Path;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.UriInfo;
import java.util.logging.Level;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentResultsBuilder {

  @EJB
  private DistributedFsService dfs;
  @EJB
  private ExperimentConfigurationConverter experimentConfigurationConverter;

  public ExperimentResultSummaryDTO uri(ExperimentResultSummaryDTO dto, UriInfo uriInfo, Project project, String mlId) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .path(mlId)
        .path(ResourceRequest.Name.RESULTS.toString().toLowerCase())
        .build());
    return dto;
  }

  public ExperimentResultSummaryDTO expand(ExperimentResultSummaryDTO dto, ResourceRequest resourceRequest) {
    if (resourceRequest != null && resourceRequest.contains(ResourceRequest.Name.RESULTS)) {
      dto.setExpand(true);
    }
    return dto;
  }

  public ExperimentResultSummaryDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project,
                                          String mlId) throws ExperimentsException {
    ExperimentResultSummaryDTO dto = new ExperimentResultSummaryDTO();
    uri(dto, uriInfo, project, mlId);
    expand(dto, resourceRequest);
    if (dto.isExpand()) {
      DistributedFileSystemOps dfso = null;
      try {
        dfso = dfs.getDfsOps();
        String summaryPath = Utils.getProjectPath(project.getName()) + Settings.HOPS_EXPERIMENTS_DATASET + "/"
            + mlId + "/.summary";
        if(dfso.exists(summaryPath)) {
          String summaryJson = dfso.cat(new Path(summaryPath));
          dto.setResults(apply(experimentConfigurationConverter.unmarshalResults(summaryJson).getResults(),
              resourceRequest.getLimit(), resourceRequest.getOffset()));

        }
      } catch (Exception e) {
        throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.RESULTS_RETRIEVAL_ERROR, Level.FINE,
            "Unable to get results for experiment", e.getMessage(), e);
      } finally {
        if (dfso != null) {
          dfs.closeDfsClient(dfso);
        }
      }
    }
    return dto;
  }

  public ExperimentResultsDTO[] apply(ExperimentResultsDTO[] dto, int limit, int offset) {

    ExperimentResultsDTO[] experimentResultsDTO = new ExperimentResultsDTO[limit];


    for(int i = 0; offset + i < offset + limit; i++) {
      experimentResultsDTO[i] = dto[offset + i];
    }

    return experimentResultsDTO;
  }
}