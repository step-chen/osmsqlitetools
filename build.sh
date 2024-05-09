#!/bin/bash
main="main.go"
appname="osmsqlitetool"
output="./build/release"
osname=`cat /etc/os-release | grep -o "^NAME=\".*\"$" | awk -F '"' '{print tolower($2)}'`
osversion=`cat /etc/os-release | grep -o "^VERSION_ID=\".*\"$" | awk -F '"' '{print $2}'`

echo "For go get connection issue at CHN to use: 
    export GOPROXY=https://goproxy.io
    or
    export GOPROXY=https://goproxy.cn,direct
For manually get module of golang use:
    go get -u github.com/mattn/go-sqlite3
    go get -u github.com/paulmach/orb
    go get -u gopkg.in/yaml.v3
For envirment issue please check env is correct config:
    export GO111MODULE=on
    export GOOS=linux
    export GOARCH=amd64
    export GOPATH=<your golang path>
    export GOROOT=<your golang root>
    export GOBIN=<your golang bin path>
    export PATH=$PATH:$GOBIN:$GOPATH

Start build dynmic release on ${osname} ${osversion}
go build -o ${output}/${appname} -ldflags '-linkmode \"external\" -extldflags \"-static\"' -tags osusergo,netgo,sqlite_omit_load_extension,sqlite_omit_load_extension ${main}"

# TODO: Still have issue with static build
#go build -o ${output}/${appname}.${osname}.${osversion} -ldflags '-s -w --extldflags "-fpic"' ${main}
#go build -o ${output}/${appname} -ldflags '-linkmode "external" -extldflags "-static"' -tags osusergo,netgo,libsqlite3,mod_spatialite ${main}
go build -o ${output}/${appname} ${main}

echo "Finished and output to: 
    ${output}/${appname}"

echo "
For using example: 
    ${appname} -f \"./samples/route1.sqlite\" -t \"./tags.yml\" -e \"./lines_extract.yml\" -s \"./lines_split.yml\""
