=begin
 This file is part of Hopsworks
 Copyright (C) 2018, Logical Clocks AB. All rights reserved

 Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 the GNU Affero General Public License as published by the Free Software Foundation,
 either version 3 of the License, or (at your option) any later version.

 Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 PURPOSE.  See the GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/>.
=end
module ExperimentResultHelper

  def create_results_experiment_job(project, job_name)

    # need to enable python for conversion .ipynb to .py works
    get "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/python/environments"
    if response.code == 404
       post "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/python/environments/3.6?action=create&pythonKernelEnable=true"
       expect_status(201)
    end

    job_conf = {
      "type":"sparkJobConfiguration",
      "appName":"#{job_name}",
      "amQueue":"default",
      "amMemory":1024,
      "amVCores":1,
      "jobType":"PYSPARK",
      "appPath":"hdfs:///Projects/#{project[:projectname]}/Jupyter/Parallel_Experiments/TensorFlow/grid_search/grid_search_fashion_mnist.ipynb",
      "mainClass":"org.apache.spark.deploy.PythonRunner",
      "spark.executor.instances":1,
      "spark.executor.cores":1,
      "spark.executor.memory":2000,
      "spark.executor.gpus":0,
      "spark.dynamicAllocation.enabled": true,
      "spark.dynamicAllocation.minExecutors":1,
      "spark.dynamicAllocation.maxExecutors":2,
      "spark.dynamicAllocation.initialExecutors":1
    }

    put "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/jobs/#{job_name}", job_conf
    expect_status(201)
  end

  def get_results(project_id, ml_id, query)
    get "#{ENV['HOPSWORKS_API']}/project/#{project_id}/experiments/#{ml_id}/#{query}"
  end
end
