#!/bin/bash

#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
module=fate_flow_server.py

# colorful
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

detect_project_base() {
    if [[ -z "${FATE_PROJECT_BASE}" ]]; then
        PROJECT_BASE=$(
            cd "$(dirname "$0")"
            cd ../
            cd ../
            pwd
        )
    else
        PROJECT_BASE="${FATE_PROJECT_BASE}"
    fi
    FATE_FLOW_BASE=${PROJECT_BASE}/fateflow
    echo "PROJECT_BASE:"
    echo "  ${PROJECT_BASE}"
}

# source init_env.sh
source_init_env() {
    INI_ENV_SCRIPT=${PROJECT_BASE}/bin/init_env.sh
    if test -f "${INI_ENV_SCRIPT}"; then
        source_init_env_error=$(source "${PROJECT_BASE}/bin/init_env.sh" 2>&1)
        if [ $? -eq 0 ]; then
            source "${PROJECT_BASE}/bin/init_env.sh"
            echo "PYTHONPATH:"
            for path in $(echo $PYTHONPATH | tr ":" "\n"); do
                echo "  ${path}"
            done
        else
            echo -e "source ${PROJECT_BASE}/bin/init_env.sh ${RED}failed${NC}:"
            echo -e "  ${RED}${source_init_env_error}${NC}"
            exit 1
        fi
    else
        echo "file not found: ${INI_ENV_SCRIPT}"
        exit 1
    fi
}

parse_yaml() {
    local prefix=$2
    local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @ | tr @ '\034')
    sed -ne "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p" $1 |
        awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}

getport() {
    service_conf_path=${PROJECT_BASE}/conf/service_conf.yaml
    if test -f "${service_conf_path}"; then
        echo "service conf:"
        echo "  ${service_conf_path}"
        eval $(parse_yaml ${service_conf_path} "service_config_")
        echo "fate flow:"
        echo "  http port: ${service_config_fateflow_http_port}"
        echo "  grpc port: ${service_config_fateflow_grpc_port}"
    else
        echo -e "${RED}service conf not found: ${service_conf_path}${NC}"
        exit 1
    fi
}

getpid() {
    pid1=$(lsof -i:${service_config_fateflow_http_port} | grep 'LISTEN' | awk 'NR==1 {print $2}')
    pid2=$(lsof -i:${service_config_fateflow_grpc_port} | grep 'LISTEN' | awk 'NR==1 {print $2}')
    if [[ -n ${pid1} && "x"${pid1} = "x"${pid2} ]]; then
        pid=$pid1
    elif [[ -z ${pid1} && -z ${pid2} ]]; then
        pid=
    fi
}

status() {
    getpid
    if [[ -n ${pid} ]]; then
        echo "status:$(ps aux | grep ${pid} | grep -v grep)"
        lsof -i:${service_config_fateflow_http_port} | grep 'LISTEN'
        lsof -i:${service_config_fateflow_grpc_port} | grep 'LISTEN'
    else
        echo -e "service ${RED}not running${NC}"
    fi
}

start() {
    echo "start:"
    log_dir=${FATE_FLOW_BASE}/logs
    mklogsdir() {
        if [[ ! -d $log_dir ]]; then
            mkdir -p $log_dir
        fi
    }
    getpid
    if [[ ${pid} == "" ]]; then
        mklogsdir
        if [[ $1x == "front"x ]]; then
            export FATE_PROJECT_BASE=${PROJECT_BASE}
            exec python ${FATE_FLOW_BASE}/python/fate_flow/fate_flow_server.py >>"${log_dir}/console.log" 2>>"${log_dir}/error.log"
            unset FATE_PROJECT_BASE
        else
            export FATE_PROJECT_BASE=${PROJECT_BASE}
            nohup python ${FATE_FLOW_BASE}/python/fate_flow/fate_flow_server.py >>"${log_dir}/console.log" 2>>"${log_dir}/error.log" &
            unset FATE_PROJECT_BASE
        fi
        sp="/-\|"
        for ((i = 1; i <= 100; i++)); do
            echo -e -n "  ${sp:i++%${#sp}:1} starting...\r"
            sleep 0.1
            getpid
            if [[ -n ${pid} ]]; then
                echo -e "  start service ${GREEN}success${NC} (pid: ${pid})"
                return
            fi
        done
        if [[ -z ${pid} ]]; then
            echo -e "  start service ${RED}failed${NC}:"
            echo "    ------"
            echo "    ..."
            tail "${log_dir}/error.log" | while read -r line; do
                echo "    ${line}"
            done
            echo "    ------"
            echo "  for full error message, please check:"
            echo "    ${log_dir}/error.log"
            echo "    ${log_dir}/console.log"
            exit 1
        fi
    else
        echo -e "  service ${GREEN}already started${NC} (pid:${pid})\n"
    fi
}

stop() {
    echo "stop:"
    getpid
    if [[ -n ${pid} ]]; then
        echo "  found process: (pid: ${pid})"
        echo "    $(ps aux | grep ${pid} | grep -v grep)"
        for ((i = 1; i <= 100; i++)); do
            echo -n "  try SIGTERM..."
            sleep 0.1
            kill ${pid}
            getpid
            if [[ ! -n ${pid} ]]; then
                echo -e "\r  stop service ${GREEN}success${NC} (SIGTERM)"
                return
            fi
        done

        echo -e "\r  try SIGKILL..."
        kill -9 ${pid}
        if [[ $? -eq 0 ]]; then
            echo -e "\r  stop service ${GREEN}success${NC} (SIGKILL)"
        else
            echo -e "\r  stop service ${RED}failed${NC}"
        fi
    else
        echo -e "  service ${RED}not running${NC}, skip"
    fi
}

split_line_start() {
    echo "----------------------------------Config--------------------------------------"
}
split_line_end() {
    echo "------------------------------------------------------------------------------"
}

init() {
    split_line_start
    detect_project_base
    if [ $# -gt 0 ]; then
        source_init_env
    fi
    getport
    split_line_end
}

case "$1" in
    start)
        init 1
        start
        ;;

    starting)
        init 1
        start front
        ;;

    stop)
        init
        stop
        ;;

    status)
        init
        status
        ;;

    restart)
        init 1
        stop

        echo -n ""
        sp="/-\|"
        for ((i = 1; i <= 100; i++)); do
            echo -e -n "\r${sp:i++%${#sp}:1} sleeping[$((i / 10))/10]"
            sleep 0.1
        done
        echo -e -n "\r                                   "
        echo -e -n "\r"

        start
        ;;
    *)
        echo "usage: $0 {start|stop|status|restart}"
        exit 1
        ;;
esac
