#!/bin/sh
. /opt/ros/jazzy/setup.sh
. /hydra_ws/install/setup.sh
. /venv/bin/activate
ros2 launch heracles_ros demo.launch.yaml launch_heracles_publisher:=true launch_hydra_visualizer:=true launch_rviz:=true
