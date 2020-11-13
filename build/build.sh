#!/usr/bin/env bash
image="maven:3.6.3-jdk-8"

# get directory of this script
# source: https://stackoverflow.com/questions/59895/how-to-get-the-source-directory-of-a-bash-script-from-within-the-script-itself
build_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# axs directory
root_dir=$(dirname ${build_dir})
# AxsUtilities directory
jar_dir="${root_dir}/AxsUtilities"
# directory to build to
target_dir="${build_dir}/target"

# build package
docker run -it -v $jar_dir:/run/axs -v $target_dir:/run/axs/target -w /run/axs $image mvn package
# fix permissions since we build as root
docker run -it -v $target_dir:/run/axs/target -w /run/axs $image chown -R $(id -u):$(id -g) /run/axs/target