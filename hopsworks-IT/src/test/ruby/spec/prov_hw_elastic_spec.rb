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

require 'pp'

describe "On #{ENV['OS']}" do
  describe 'provenance tests - experiments' do
    before :all do
      with_valid_session
    end
    def create_dir(project, dirname) 
      target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset"
      payload_string = '{"name": "' + dirname + '"}'
      payload = JSON.parse(payload_string)
      post target, payload
      expect_status(200)
    end

    def delete_dir(project, dirname) 
      target = "#{ENV['HOPSWORKS_API']}/project/#{project[:id]}/dataset/file/#{dirname}"
      delete target
      expect_status(200)
    end

    def get_experiments() 
      ml_type = "EXPERIMENT"
      target = "#{ENV['HOPSWORKS_API']}/provenance/mlType/#{ml_type}"
      result = get target
      expect_status(200)
      parsed_result = JSON.parse(result)
    end

    def get_experiments_in_project(project) 
      ml_type = "EXPERIMENT"
      target = "#{ENV['HOPSWORKS_API']}/provenance/mlType/#{ml_type}/project/#{project[:id]}"
      result = get target
      expect_status(200)
      parsed_result = JSON.parse(result)
    end

    def get_experiment_by_id(project, mlId) 
      ml_type = "EXPERIMENT"
      target = "#{ENV['HOPSWORKS_API']}/provenance/mlType/#{ml_type}/project/#{project[:id]}/mlId/#{mlId}"
      result = get target
      expect_status(200)
      parsed_result = JSON.parse(result)
    end

    def check_experiment(experiments, project, experimentName) 
      experiment = experiments.select {|exp| exp["inode_name"] == experimentName}
      expect(experiment.length).to eq 1
      pp experiment
    end

    def get_experiment_id(experiments, experimentName)
      experiment = experiments.select {|exp| exp["inode_name"] == experimentName}
      expect(experiment.length).to eq 1
      experiment[0]["mlId"]
    end

    def xattr_add(original, xattr_name, xattr_value, increment)
      xattrRecord = original.dup
      xattrRecord["inode_operation"] = "XATTR_ADD"
      xattrRecord["io_logical_time"] = original["io_logical_time"]+increment
      xattrRecord["io_timestamp"] = original["io_timestamp"]+increment
      xattrRecord["i_xattr_name"] = xattr_name
      xattrRecord["io_logical_time_batch"] = original["io_logical_time_batch"]+increment
      xattrRecord["io_timestamp_batch"] = original["io_timestamp_batch"]+increment
      xattrRecord.save!
      
      FileProvXAttr.create(inode_id: xattrRecord["inode_id"], namespace: 5, name: xattr_name, inode_logical_time: xattrRecord["io_logical_time"], value: xattr_value)
    end

    def wait_for_epipe() 
      pp "waiting"
      sleepCounter = 0
      until FileProv.all.empty? || sleepCounter == 10 do
        sleep(5)
        sleepCounter += 1
      end
      expect(sleepCounter).to be < 10
      pp "done waiting"
    end

    it "test experiments with no xattr" do 
      project1Name = "prov_proj_#{short_random_id}"
      pp "create project: #{project1Name}"
      project1 = create_project_by_name(project1Name)

      project2Name = "prov_proj_#{short_random_id}"
      pp "create project: #{project2Name}"
      project2 = create_project_by_name(project2Name)

      experiment1Name="Experiment_A_V_1"
      pp "create experiment #{experiment1Name} in project #{project1Name}"
      create_dir(project1, "Experiments/#{experiment1Name}")

      experiment2Name="Experiment_A_V_2"
      pp "create experiment #{experiment2Name} in project #{project2Name}"
      create_dir(project2, "Experiments/#{experiment2Name}")

      experiment3Name="Experiment_A_V_3"
      pp "create experiment #{experiment3Name} in project #{project2Name}"
      create_dir(project2, "Experiments/#{experiment3Name}")

      wait_for_epipe()

      pp "query experiments in project #{project1Name}"
      result2 = get_experiments_in_project(project1)
      expect(result2.length).to eq 1
      check_experiment(result2, project1, experiment1Name)

      pp "query experiments in project #{project2Name}"
      result3 = get_experiments_in_project(project2)
      expect(result3.length).to eq 2
      check_experiment(result3, project2, experiment2Name)
      check_experiment(result3, project2, experiment3Name)

      experiment1Id = get_experiment_id(result2, experiment1Name)
      pp "query experiment by id - project #{project1Name} experiment #{experiment1Name} id #{experiment1Id}"
      result4 = get_experiment_by_id(project1, experiment1Id)
      pp result4
      check_experiment(result4, project1, experiment1Name)

      pp "delete experiment #{experiment1Name} in project #{project1Name}"
      delete_dir(project1, "Experiments/#{experiment1Name}")
      pp "delete experiment #{experiment2Name} in project #{project2Name}"
      delete_dir(project2, "Experiments/#{experiment2Name}")
      pp "delete experiment #{experiment3Name} in project #{project2Name}"
      delete_dir(project2, "Experiments/#{experiment3Name}")

      wait_for_epipe()

      pp "query experiments in project #{project1Name}"
      result5 = get_experiments_in_project(project1)
      expect(result5.length).to eq 0

      pp "query experiments in project #{project2Name}"
      result6 = get_experiments_in_project(project2)
      expect(result6.length).to eq 0

      pp "delete project: #{project1Name}"
      delete_project(project1)
      pp "delete project: #{project2Name}"
      delete_project(project2)
    end

    it "test experiment with xattr" do
      execute_remotely @hostname, "sudo systemctl stop epipe"
      pp "stopped epipe"

      projectName = "prov_proj_test_#{short_random_id}"
      pp "create project: #{projectName}"
      project = create_project_by_name(projectName)

      experimentName="Experiment_A_V_1"
      pp "create experiment #{experimentName} in project #{projectName}"
      create_dir(project, "Experiments/#{experimentName}")
      
      
      experimentRecord = FileProv.where("i_name": experimentName)
      expect(experimentRecord.length).to eq 1

      xattr_add(experimentRecord[0], "xattr_key_1", "xattr_value_1", 1)
      xattr_add(experimentRecord[0], "xattr_key_2", "xattr_value_2", 2)

      execute_remotely @hostname, "sudo systemctl restart epipe"
      pp "restarted epipe"

      wait_for_epipe()

      pp "query experiments in project #{projectName}"
      result = get_experiments_in_project(project)
      expect(result.length).to eq 1
      check_experiment(result, project, experimentName)

      pp "delete experiment #{experimentName} in project #{projectName}"
      delete_dir(project, "Experiments/#{experimentName}")

      pp "delete project: #{projectName}"
      delete_project(project)
    end
  end
end