#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CONNECTOR_SAMPLES_DIR=../..

UBER_JAR=`echo ${CONNECTOR_SAMPLES_DIR}/target/edgent-samples-connectors-*-uber.jar`

# Runs the File connector sample
#
# ./runfilesample.sh writer
# ./runfilesample.sh reader

sampledir=/tmp/fileConnectorSample
if [ ! -e $sampledir ]; then
    mkdir $sampledir
fi 

export CLASSPATH=${UBER_JAR}

app=$1; shift
if [ "$app" == "writer" ]; then
    java org.apache.edgent.samples.connectors.file.FileWriterApp $sampledir
elif [ "$app" == "reader" ]; then
    java org.apache.edgent.samples.connectors.file.FileReaderApp $sampledir
else
    echo "unrecognized mode '$app'"
    echo "usage: $0 'writer|reader'"
    exit 1
fi
