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
    @app1_id = "application_#{short_random_id}_0001"
    @app2_id = "application_#{short_random_id}_0001"
    @app3_id = "application_#{short_random_id}_0001"
    @experiment_app1_name1 = "#{@app1_id}_1"
    @experiment_app2_name1 = "#{@app2_id}_1"
    @experiment_app3_name1 = "#{@app3_id}_1"
    @experiment_app1_name2 = "#{@app1_id}_2"
    @not_experiment_name = "not_experiment"
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

  def get_ml_asset_in_project(project, ml_type, withAppState) 
    target = "#{ENV['HOPSWORKS_API']}/provenance/mlType/#{ml_type}/project/#{project[:id]}?withAppState=#{withAppState}"
    pp "#{target}"
    result = get target
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def get_ml_asset_by_id(project, ml_type, ml_id, withAppState) 
    target = "#{ENV['HOPSWORKS_API']}/provenance/mlType/#{ml_type}/project/#{project[:id]}/mlId/#{ml_id}?withAppState=#{withAppState}"
    pp "#{target}"
    result = get target
    expect_status(200)
    parsed_result = JSON.parse(result)
  end

  def add_xattr(original, xattr_name, xattr_value, xattr_op, increment)
    xattrRecord = original.dup
    xattrRecord["inode_operation"] = xattr_op
    xattrRecord["io_logical_time"] = original["io_logical_time"]+increment
    xattrRecord["io_timestamp"] = original["io_timestamp"]+increment
    xattrRecord["i_xattr_name"] = xattr_name
    xattrRecord["io_logical_time_batch"] = original["io_logical_time_batch"]+increment
    xattrRecord["io_timestamp_batch"] = original["io_timestamp_batch"]+increment
    xattrRecord.save!

    FileProvXAttr.create(inode_id: xattrRecord["inode_id"], namespace: 5, name: xattr_name, inode_logical_time: xattrRecord["io_logical_time"], value: xattr_value)
  end

  def add_app_states(app_id, user)
    timestamp = Time.now
    AppProv.create(id: app_id, state: "null", timestamp: timestamp, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "null", timestamp: timestamp+5, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: 0)
    AppProv.create(id: app_id, state: "FINISHED", timestamp: timestamp+10, name: app_id, user: user, submit_time: timestamp-10, start_time: timestamp-5, finish_time: timestamp+50)
  end

  def wait_for_epipe() 
    pp "waiting"
    sleepCounter1 = 0
    sleepCounter2 = 0
    until FileProv.all.empty? || sleepCounter1 == 10 do
      sleep(5)
      sleepCounter1 += 1
    end
    until AppProv.all.empty? || sleepCounter2 == 10 do
      sleep(5)
      sleepCounter2 += 1
    end
    sleep(5)
    expect(sleepCounter1).to be < 10
    expect(sleepCounter2).to be < 10
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
      "#{experiment_name}"
    end

    def check_experiment(experiments, experiment_id, xattrs, app_states) 
      experiment = experiments.select { |e| e["mlId"] == experiment_id }
      expect(experiment.length).to eq 1
      #pp experiment
      expect(experiment[0]["xattrs"]["entry"].length).to eq xattrs.length
      xattrs.each do |key, value|
        # pp experiment[0]["xattrs"]["entry"]
        xattr = experiment[0]["xattrs"]["entry"].select do |e| 
          e["key"] == key && e["value"] == value
        end
        expect(xattr.length).to eq 1
        #pp xattr
      end
      expect(experiment[0]["appStates"]["entry"].length).to eq app_states.length
      app_states.each do | key |
        #pp experiment[0]["appStates"]["entry"]
        state = experiment[0]["appStates"]["entry"].select do |e|
          e["key"] == key
        end
        expect(state.length).to eq 1
        #pp state
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

    def check_model(models, model_id, xattrs) 
      model = models.select {|m| m["mlId"] == model_id }
      expect(model.length).to eq 1
      #pp model
      xattrs.each do |key, value|
        pp model[0]["xattrs"]["entry"]
        xattr = model[0]["xattrs"]["entry"].select do |e| 
          e["key"] == key && e["value"] == value
        end
        expect(xattr.length).to eq 1
        pp xattr
      end
    end
  end
end