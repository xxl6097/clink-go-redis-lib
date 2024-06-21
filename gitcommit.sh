#!/bin/bash
function todir() {
  pwd
}

function pull() {
  todir
  echo "git pull"
  git pull
}

function forcepull() {
  todir
  echo "git fetch --all && git reset --hard origin/master && git pull"
  git fetch --all && git reset --hard origin/master && git pull
}


#  shellcheck disable=SC2120
function gitpush() {
  commit=""
  if [ ! -n "$1" ]; then
    commit="$(date '+%Y-%m-%d %H:%M:%S') by ${USER}"
  else
    commit="$1 by ${USER}"
  fi

  echo $commit
  git add .
  git commit -m "$commit"
  #  git push -u origin main
  git push
}

function m() {
    echo "1. 强制更新"
    echo "2. 普通更新"
    echo "3. 提交项目"
    echo "4. 构建镜像"
    echo "请输入编号:"
    read index

    case "$index" in
    [1]) (forcepull);;
    [2]) (pull);;
    [3]) (gitpush);;
    [4]) (createimage);;
    *) echo "exit" ;;
  esac
}

function bootstrap() {
    case $1 in
    pull) (pull) ;;
    m) (m) ;;
      -f) (forcepull) ;;
       *) ( gitpush $1)  ;;
    esac
}

function createimage() {
    gitpush
    curl -u pt946a3zc74l:7e525b20f5db96b707d1efad1419bd6c5a57f6ee \
       -v -X POST  'https://clife-devops.coding.net/api/cci/job/3843855/trigger' \
       -H 'Content-Type: application/json' \
       -d '
        {
        "ref": "master",
        "envs": [
            {
                "name": "K8S_CRED",
                "value": "ec08644f-9c70-40ed-b2a8-13d70fb06b33",
                "sensitive": 0
            },
            {
                "name": "DOCKER_CRED",
                "value": "3a602514-e764-436f-a6ef-7c2403f5b146",
                "sensitive": 0
            },
            {
                "name": "DEPLOY_ENV",
                "value": "testsl",
                "sensitive": 0
            },
            {
                "name": "K8S_SERVER_URL",
                "value": "https://121.15.143.68:6443",
                "sensitive": 0
            },
            {
                "name": "SERVICE_NAMES",
                "value": "clink-go-tcp-server",
                "sensitive": 0
            },
            {
                "name": "SERVICE_PORT",
                "value": "31380",
                "sensitive": 0
            },
            {
                "name": "ServiceVersion",
                "value": "v0.0.0",
                "sensitive": 0
            },
            {
                "name": "NAMESPACE",
                "value": "clink",
                "sensitive": 0
            },
            {
                "name": "GO_VERSION",
                "value": "1.19",
                "sensitive": 0
            },
            {
                "name": "VERSION_TYPE",
                "value": "snapshot",
                "sensitive": 0
            },
            {
                "name": "PRIVATE_CLOUD",
                "value": "clife",
                "sensitive": 0
            },
            {
                "name": "SERVICE_TCP_PORT",
                "value": "31300",
                "sensitive": 0
            }
        ]
    }'
}


bootstrap m
