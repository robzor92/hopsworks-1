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
module TensorBoardHelper
  def create_tensorboard(project_id, ml_id)
    post "#{ENV['HOPSWORKS_API']}/project/#{project_id}/experiments/#{ml_id}/tensorboard"
  end

  def get_tensorboard(project_id, ml_id)
    get "#{ENV['HOPSWORKS_API']}/project/#{project_id}/experiments/#{ml_id}/tensorboard"
  end

  def delete_tensorboard(project_id, ml_id)
    delete "#{ENV['HOPSWORKS_API']}/project/#{project_id}/experiments/#{ml_id}/tensorboard"
  end
end
