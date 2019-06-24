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
  before :all do
    $stdout.sync = true
    with_valid_session
    @project1_name = "prov_proj_#{short_random_id}"
    @project2_name = "prov_proj_#{short_random_id}"
    @experiment1_name = "experiment_a_v_1"
    @experiment2_name = "experiment_a_v_2"
    @experiment3_name = "experiment_a_v_3"
    @model1_name = "model_a"
    @model2_name = "model_b"
    @model_version1 = "1"
    @model_version2 = "2"
    pp "create project: #{@project1_name}"
    @project1 = create_project_by_name(@project1_name)
    pp "create project: #{@project2_name}"
    @project2 = create_project_by_name(@project2_name)
  end

  after :all do 
    pp "delete projects"
    delete_project(@project1)
    delete_project(@project2)
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

  def get_ml_asset_in_project(project, ml_type) 
    target = "#{ENV['HOPSWORKS_API']}/provenance/mlType/#{ml_type}/project/#{project[:id]}"
    pp "#{target}"
    result = get target
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_ml_asset_by_id(project, ml_type, ml_id) 
    target = "#{ENV['HOPSWORKS_API']}/provenance/mlType/#{ml_type}/project/#{project[:id]}/mlId/#{ml_id}"
    pp "#{target}"
    result = get target
    expect_status(200)
    parsed_result = JSON.parse(result)
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
    sleep(5)
    expect(sleepCounter).to be < 10
    pp "done waiting"
  end
  
  describe 'provenance tests - experiments' do
    def create_experiment(project, experiment_name) 
      pp "create experiment #{experiment_name} in project #{project[:inode_name]}"
      create_dir(project, "Experiments/#{experiment_name}")
    end
    
    def delete_experiment(project, experiment_name) 
      pp "delete experiment #{experiment_name} in project #{project[:inode_name]}"
      delete_dir(project, "Experiments/#{experiment_name}")
    end
    
    def experiment_ml_id(experiment_name)
      id = "#{experiment_name}"
      pp id
      id
    end

    def check_experiment(experiments, experiment_id) 
      experiment = experiments.select {|e| e[:mlId] == experiment_id }
      expect(experiment.length).to eq 1
      pp experiment
    end
    
    describe 'simple experiment' do
      it "create experiments" do
        create_experiment(@project1, @experiment1_name)
        create_experiment(@project1, @experiment2_name)
        create_experiment(@project2, @experiment3_name)
      end

      it "check experiments" do 
        wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT")
        expect(result1.length).to eq 2
        check_experiment(result1, experiment_ml_id(@experiment1_name))
        check_experiment(result1, experiment_ml_id(@experiment2_name))

        result2 = get_ml_asset_in_project(@project2, "EXPERIMENT")
        expect(result2.length).to eq 1
        check_experiment(result2, experiment_ml_id(@experiment3_name))
        
        result3 = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_ml_id(@experiment1_name))
        expect(result3.length).to eq 1
        check_experiment(result3, experiment_ml_id(@experiment1_name))
      end

      it "delete experiments" do
        delete_experiment(@project1, @experiment1_name)
        delete_experiment(@project1, @experiment2_name)
        delete_experiment(@project2, @experiment3_name)
      end
      
      it "check experiments" do 
        wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "EXPERIMENT")
        expect(result1.length).to eq 0

        result2 = get_ml_asset_in_project(@project2, "EXPERIMENT")
        expect(result2.length).to eq 0
        
        result3 = get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_ml_id(@experiment1_name))
        expect(result3.length).to eq 0
      end
    end
  end

  describe 'provenance tests - models' do
    def create_model1(project, model_name) 
      pp "create model #{model_name} in project #{project[:inode_name]}"
      create_dir(project, "Models/#{model_name}")
    end

    def create_model2(project, model_name, model_version) 
      pp "create model #{model_name}_#{model_version} in project #{project[:inode_name]}"
      create_dir(project, "Models/#{model_name}/#{model_version}")
    end
    
    def delete_model(project, model_name) 
      pp "delete model #{model_name} in project #{project[:inode_name]}"
      delete_dir(project, "Models/#{model_name}")
    end
    
    def model_ml_id(model_name, model_version)
      "#{model_name}_#{model_version}"
    end

    def check_model(models, model_id) 
      model = models.select {|m| m[:mlId] == model_id }
      expect(model.length).to eq 1
      pp model
    end
    
    describe 'simple models' do
      it "create models" do
        create_model1(@project1, @model1_name)
        create_model2(@project1, @model1_name, @model_version1)
        create_model2(@project1, @model1_name, @model_version2)
        create_model1(@project1, @model2_name)
        create_model2(@project1, @model2_name, @model_version1)
        create_model1(@project2, @model1_name)
        create_model2(@project2, @model1_name, @model_version1)
      end

      it "check models" do 
        wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL")
        expect(result1.length).to eq 3
        check_model(result1, model_ml_id(@model1_name, @model_version1))
        check_model(result1, model_ml_id(@model1_name, @model_version2))
        check_model(result1, model_ml_id(@model2_name, @model_version1))

        result2 = get_ml_asset_in_project(@project2, "MODEL")
        expect(result2.length).to eq 1
        check_model(result2, model_ml_id(@model1_name, @model_version1))
        
        result3 = get_ml_asset_by_id(@project1, "MODEL", model_ml_id(@model1_name, @model_version2))
        expect(result3.length).to eq 1
        check_model(result3, model_ml_id(@model1_name, @model_version2))
      end

      it "delete models" do
        delete_model(@project1, @model1_name)
        delete_model(@project1, @model2_name)
        delete_model(@project2, @model1_name)
      end
      
      it "check models" do 
        wait_for_epipe() 
        result1 = get_ml_asset_in_project(@project1, "MODEL")
        expect(result1.length).to eq 0

        result2 = get_ml_asset_in_project(@project2, "MODEL")
        expect(result2.length).to eq 0
        
        result3 = get_ml_asset_by_id(@project1, "MODEL", model_ml_id(@model1_name, @model_version2))
        expect(result3.length).to eq 0
      end
    end
  end
end