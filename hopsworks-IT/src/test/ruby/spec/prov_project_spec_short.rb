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
    @td1_name = "td_a"
    @td2_name = "td_b"
    @td_version1 = "1"
    @td_version2 = "2"
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

  describe 'simple training datasets' do
    it "create training datasets" do
      prov_create_td(@project1, @td1_name, @td_version1)
      prov_create_td(@project1, @td1_name, @td_version2)
      prov_create_td(@project1, @td2_name, @td_version1)
      prov_create_td(@project2, @td1_name, @td_version1)
    end

    it "check training datasets" do 
      prov_wait_for_epipe() 
      result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
      expect(result1.length).to eq 3
      prov_check_td1(result1, @td1_name, @td_version1)
      prov_check_td1(result1, @td1_name, @td_version2)
      prov_check_td1(result1, @td2_name, @td_version1)

      result2 = get_ml_asset_in_project(@project2, "TRAINING_DATASET", false)
      expect(result2.length).to eq 1
      prov_check_td1(result2, @td1_name, @td_version1)
      
      result3 = get_ml_asset_by_id(@project1, "TRAINING_DATASET", prov_td_id(@td1_name, @td_version1), false, 200)
    end

    it "delete training datasets" do
      prov_delete_td(@project1, @td1_name, @td_version1)
      prov_delete_td(@project1, @td1_name, @td_version2)
      prov_delete_td(@project1, @td2_name, @td_version1)
      prov_delete_td(@project2, @td1_name, @td_version1)
    end
    
    it "check training datasets" do 
      prov_wait_for_epipe() 
      result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
      expect(result1.length).to eq 0

      result2 = get_ml_asset_in_project(@project2, "TRAINING_DATASET", false)
      expect(result2.length).to eq 0
      
      result3 = get_ml_asset_by_id(@project1, "TRAINING_DATASET", prov_td_id(@td1_name, @td_version1), false, 404)
    end
  end
  describe 'training dataset with xattr' do
    it "stop epipe" do
      execute_remotely @hostname, "sudo systemctl stop epipe"
    end

    it "create training dataset with xattr" do
      prov_create_td(@project1, @td1_name, @td_version1)
      td_record = prov_get_td_record(@project1, @td1_name, @td_version1)
      expect(td_record.length).to eq 1
      prov_add_xattr(td_record[0], "xattr_key_1", "xattr_value_1", "XATTR_ADD", 1)
      prov_add_xattr(td_record[0], "xattr_key_2", "xattr_value_2", "XATTR_ADD", 2)
    end

    it "restart epipe" do
      execute_remotely @hostname, "sudo systemctl restart epipe"
    end

    it "check model" do 
      prov_wait_for_epipe() 
      result1 = get_ml_asset_in_project(@project1, "TRAINING_DATASET", false)
      expect(result1.length).to eq 1
      xattrs = Hash.new
      xattrs["xattr_key_1"] = "xattr_value_1"
      xattrs["xattr_key_2"] = "xattr_value_2"
      prov_check_model(result1, prov_model_id(@model1_name, @model_version1), xattrs)
    end

    it "delete model" do
      prov_delete_model(@project1, @model1_name)
    end

    it "check models" do 
      prov_wait_for_epipe() 
      result1 = get_ml_asset_in_project(@project1, "MODEL", false)
      expect(result1.length).to eq 0
    end
  end
end