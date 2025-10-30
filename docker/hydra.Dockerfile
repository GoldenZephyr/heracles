FROM ros:jazzy-ros-core

RUN apt-get update
RUN apt-get install -y git ros-dev-tools python3.12-venv python3-pip libgoogle-glog-dev
RUN rosdep init && rosdep update

RUN mkdir -p /hydra_ws/src
WORKDIR /hydra_ws
RUN echo "build: {cmake-args: [-DCMAKE_BUILD_TYPE=Release]}" > colcon_defaults.yaml

WORKDIR /hydra_ws/src
RUN git clone https://github.com/MIT-SPARK/Hydra-ROS.git hydra_ros
RUN git clone https://github.com/GoldenZephyr/heracles_agents.git
RUN git clone https://github.com/GoldenZephyr/heracles.git

RUN python3 -m venv /venv --system-site-packages
RUN git clone https://github.com/MIT-SPARK/config_utilities.git
RUN git clone --branch ros2 https://github.com/MIT-SPARK/Kimera-PGMO.git
RUN git clone https://github.com/MIT-SPARK/Spark-DSG.git
RUN git clone https://github.com/MIT-SPARK/ianvs.git

RUN rosdep install --from-paths . --ignore-src -r -y
WORKDIR /hydra_ws
RUN . /opt/ros/jazzy/setup.sh && colcon build --packages-up-to hydra_visualizer && colcon build --packages-up-to heracles heracles_ros heracles_agents
RUN . /venv/bin/activate && pip install ./src/heracles_agents[all]
COPY run_hydra_visualization.sh /run_hydra_visualization.sh
