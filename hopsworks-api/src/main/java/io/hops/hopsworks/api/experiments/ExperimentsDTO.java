package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.jobs.JobDTO;
import io.hops.hopsworks.common.api.RestDTO;
import io.hops.hopsworks.common.experiments.ExperimentConfiguration;

public class ExperimentsDTO extends RestDTO<JobDTO> {

    private String id;
    private ExperimentConfiguration experimentConfiguration;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
