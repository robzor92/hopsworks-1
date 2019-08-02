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
    pp "user email: #{@user["email"]}"
    @project1_name = "prov_proj_#{short_random_id}"
    @project2_name = "prov_proj_#{short_random_id}"
    @app1_id = "application_#{short_random_id}_0001"
    @app2_id = "application_#{short_random_id}_0001"
    @app3_id = "application_#{short_random_id}_0001"
    @experiment_app1_name1 = "#{@app1_id}_1"
    @experiment_app2_name1 = "#{@app2_id}_1"
    @experiment_app3_name1 = "#{@app3_id}_1"
    @experiment_app1_name2 = "#{@app1_id}_2"
    @experiment_app1_name3 = "#{@app1_id}_3"
    @app_file_history1   = "application_#{short_random_id}_fh1"
    @app_file_history2   = "application_#{short_random_id}_fh2"
    @app_file_history3   = "application_#{short_random_id}_fh3"
    @app_file_ops1      = "application_#{short_random_id}_fo1"
    @app_file_ops2      = "application_#{short_random_id}_fo2"
    @app_app_fprint     = "application_#{short_random_id}_af"
    @experiment_file_history    = "#{@app_file_history1}_1"
    @experiment_file_ops        = "#{@app_file_ops1}_1"
    @experiment_app_fprint      = "#{@app_app_fprint}_1"
    @not_experiment_name = "not_experiment"
    @model1_name = "model_a"
    @model2_name = "model_b"
    @model_version1 = "1"
    @model_version2 = "2"
    @td1_name = "td_a"
    @td2_name = "td_b"
    @td_version1 = "1"
    @td_version2 = "2"
    @xattrV1 = JSON['{"f1_1":"v1","f1_2":{"f2_1":"val1"}}']
    @xattrV2 = JSON['{"f1_1":"v1","f1_2":{"f2_2":"val2"}}']
    @xattrV3 = JSON['[{"f3_1":"val1","f3_2":"val2"},{"f4_1":"val3","f4_2":"val4"}]']
    @xattrV4 = "notJson"
    @xattrV5 = JSON['[{"f3_1":"val1","f3_2":"val1"},{"f3_1":"val2","f3_2":"val2"}]']
    @xattrV6 = "notJava"
    @xattrV7 = "not Json"
    @xattrV8 = JSON['{"name": "fashion mnist gridsearch"}']
    pp "create project: #{@project1_name}"
    @project1 = create_project_by_name(@project1_name)
    pp "create project: #{@project2_name}"
    @project2 = create_project_by_name(@project2_name)
  end

  #after :all do 
  #  pp "delete projects"
  #  delete_project(@project1)
  #end

  describe 'mock app file operations' do
    it "stop epipe" do
      execute_remotely @hostname, "sudo systemctl stop epipe"
    end

    it "create mock file operations - file operations" do
      prov_create_experiment(@project1, @experiment_file_ops)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_file_ops)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_file_ops1}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 100000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_file_ops1
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = 1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_2_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_2_op_1["inode_id"] = 100001
      experiment_mock_file_2_op_1["io_timestamp"] = experiment_mock_file_2_op_1["io_timestamp"]+1
      experiment_mock_file_2_op_1["i_name"] = "mock_file_2"
      experiment_mock_file_2_op_1.save!

      experiment_mock_file_3_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_3_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_3_op_1["inode_id"] = 100002
      experiment_mock_file_3_op_1["io_timestamp"] = experiment_mock_file_3_op_1["io_timestamp"]+1
      experiment_mock_file_3_op_1["io_app_id"] = @app_file_ops2
      experiment_mock_file_3_op_1["i_name"] = "mock_file_3"
      experiment_mock_file_3_op_1.save!
    end

    it "restart epipe" do
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 
    end

    it "check mock fileOperations" do 
      result = get_app_fileOperations(@project1, @app_file_ops1, "FULL")
      # pp result
      expect(result.length).to eq 3

      result = get_app_fileOperations(@project1, @app_file_ops1, "COMPACT")
      # pp result
      expect(result.length).to eq 2
    end

    it "delete experiment dataset" do
      prov_delete_experiment(@project1, @experiment_file_ops)
    end

    it "check cleanup" do 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_file_ops)
      get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false, 404)
    end
  end

  describe 'mock app footprint' do
    it "stop epipe" do
      execute_remotely @hostname, "sudo systemctl stop epipe"
    end

    it "create mock file operations - app footprint" do
      prov_create_experiment(@project1, @experiment_app_fprint)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_app_fprint)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_app_fprint}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 200000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_out_added_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_app_fprint
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = experiment_mock_file_1_op_2["io_logical_time"]+1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_2_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_2_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_2_op_1["inode_id"] = 200001
      experiment_mock_file_2_op_1["io_logical_time"] = 0
      experiment_mock_file_2_op_1["io_timestamp"] = experiment_mock_file_2_op_1["io_timestamp"]+1
      experiment_mock_file_2_op_1["i_name"] = "mock_file_out_added_2"
      experiment_mock_file_2_op_1.save!

      experiment_mock_file_3_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_3_op_1["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_3_op_1["inode_id"] = 200002
      experiment_mock_file_3_op_1["io_logical_time"] = 1
      experiment_mock_file_3_op_1["io_timestamp"] = experiment_mock_file_3_op_1["io_timestamp"]+1
      experiment_mock_file_3_op_1["i_name"] = "mock_file_out_1"
      experiment_mock_file_3_op_1.save!

      experiment_mock_file_4_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_4_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_4_op_1["inode_id"] = 200003
      experiment_mock_file_4_op_1["io_logical_time"] = 0
      experiment_mock_file_4_op_1["io_timestamp"] = experiment_mock_file_4_op_1["io_timestamp"]+1
      experiment_mock_file_4_op_1["i_name"] = "mock_file_tmp_1"
      experiment_mock_file_4_op_1.save!

      experiment_mock_file_4_op_2 = experiment_mock_file_4_op_1.dup
      experiment_mock_file_4_op_2["inode_operation"] = "DELETE"
      experiment_mock_file_4_op_2["io_logical_time"] = experiment_mock_file_4_op_2["io_logical_time"]+1
      experiment_mock_file_4_op_2["io_timestamp"] = experiment_mock_file_4_op_2["io_timestamp"]+1
      experiment_mock_file_4_op_2.save!

      experiment_mock_file_5_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_5_op_1["inode_operation"] = "DELETE"
      experiment_mock_file_5_op_1["inode_id"] = 200004
      experiment_mock_file_5_op_1["io_logical_time"] = 1
      experiment_mock_file_5_op_1["io_timestamp"] = experiment_mock_file_5_op_1["io_timestamp"]+1
      experiment_mock_file_5_op_1["i_name"] = "mock_file_rm_1"
      experiment_mock_file_5_op_1.save!

      experiment_mock_file_6_op_1 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_6_op_1["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_6_op_1["inode_id"] = 200005
      experiment_mock_file_6_op_1["io_logical_time"] = 1
      experiment_mock_file_6_op_1["io_timestamp"] = experiment_mock_file_6_op_1["io_timestamp"]+1
      experiment_mock_file_6_op_1["i_name"] = "mock_file_in_1"
      experiment_mock_file_6_op_1.save!
    end

    it "restart epipe" do
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 
    end

    it "check mock app footprint" do 
      result = get_app_footprint(@project1, @app_app_fprint, "ALL")
      # pp result
      expect(result.length).to eq 6

      result = get_app_footprint(@project1, @app_app_fprint, "INPUT")
      # pp result
      expect(result.length).to eq 1

      result = get_app_footprint(@project1, @app_app_fprint, "OUTPUT")
      # pp result
      expect(result.length).to eq 3

      result = get_app_footprint(@project1, @app_app_fprint, "OUTPUT_ADDED")
      # pp result
      expect(result.length).to eq 2

      result = get_app_footprint(@project1, @app_app_fprint, "TMP")
      # pp result
      expect(result.length).to eq 1
    end

    it "delete experiment dataset" do
      prov_delete_experiment(@project1, @experiment_app_fprint)
    end

    it "check cleanup" do 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_app_fprint)
      get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false, 404)
    end
  end

  describe 'mock file history' do
    it "stop epipe" do
      execute_remotely @hostname, "sudo systemctl stop epipe"
    end

    it "create mock file history" do
      prov_create_experiment(@project1, @experiment_file_history)
      experiment_record = FileProv.where("project_name": @project1["inode_name"], "i_name": @experiment_file_history)
      expect(experiment_record.length).to eq 1
      prov_add_xattr(experiment_record[0], "appId", "#{@app_file_history1}", "XATTR_ADD", 1)
      #pp experiment_record

      experiment_mock_file_1_op_1 = experiment_record[0].dup
      experiment_mock_file_1_op_1["inode_operation"] = "CREATE"
      experiment_mock_file_1_op_1["inode_id"] = 300000
      experiment_mock_file_1_op_1["io_logical_time"] = 0
      experiment_mock_file_1_op_1["io_timestamp"] = experiment_mock_file_1_op_1["io_timestamp"]+1
      experiment_mock_file_1_op_1["parent_i_id"] = experiment_record[0]["inode_id"]
      experiment_mock_file_1_op_1["i_name"] = "mock_file_1"
      experiment_mock_file_1_op_1["i_parent_name"] = experiment_record[0]["i_name"]
      experiment_mock_file_1_op_1["i_p1_name"] = experiment_record[0]["i_parent_name"]
      experiment_mock_file_1_op_1["io_app_id"] = @app_file_history1
      experiment_mock_file_1_op_1.save!

      experiment_mock_file_1_op_2 = experiment_mock_file_1_op_1.dup
      experiment_mock_file_1_op_2["inode_operation"] = "MODIFY_DATA"
      experiment_mock_file_1_op_2["io_logical_time"] = experiment_mock_file_1_op_2["io_logical_time"]+1
      experiment_mock_file_1_op_2["io_timestamp"] = experiment_mock_file_1_op_2["io_timestamp"]+1
      experiment_mock_file_1_op_2.save!

      experiment_mock_file_1_op_3 = experiment_mock_file_1_op_2.dup
      experiment_mock_file_1_op_3["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_1_op_3["io_app_id"] = @app_file_history2
      experiment_mock_file_1_op_3["io_logical_time"] = experiment_mock_file_1_op_3["io_logical_time"]+1
      experiment_mock_file_1_op_3["io_timestamp"] = experiment_mock_file_1_op_3["io_timestamp"]+1
      experiment_mock_file_1_op_3.save!

      experiment_mock_file_1_op_4 = experiment_mock_file_1_op_3.dup
      experiment_mock_file_1_op_4["inode_operation"] = "ACCESS_DATA"
      experiment_mock_file_1_op_4["io_app_id"] = @app_file_history3
      experiment_mock_file_1_op_4["io_logical_time"] = experiment_mock_file_1_op_4["io_logical_time"]+1
      experiment_mock_file_1_op_4["io_timestamp"] = experiment_mock_file_1_op_4["io_timestamp"]+1
      experiment_mock_file_1_op_4.save!

      experiment_mock_file_1_op_5 = experiment_mock_file_1_op_4.dup
      experiment_mock_file_1_op_5["inode_operation"] = "DELETE"
      experiment_mock_file_1_op_5["io_logical_time"] = experiment_mock_file_1_op_5["io_logical_time"]+1
      experiment_mock_file_1_op_5["io_timestamp"] = experiment_mock_file_1_op_5["io_timestamp"]+1
      experiment_mock_file_1_op_5.save!
    end

    it "restart epipe" do
      execute_remotely @hostname, "sudo systemctl restart epipe"
      prov_wait_for_epipe() 
    end

    it "check file history" do 
      result = get_file_history(@project1, 300000, "FULL")
      # pp result
      expect(result.length).to eq 5
    end

    it "delete experiment dataset" do
      prov_delete_experiment(@project1, @experiment_file_history)
    end

    it "check cleanup" do 
      prov_wait_for_epipe() 
      experiment_id1 = prov_experiment_id(@experiment_file_history)
      get_ml_asset_by_id(@project1, "EXPERIMENT", experiment_id1, false, 404)
    end
  end
end