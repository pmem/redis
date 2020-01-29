#!/bin/bash
#
#  Copyright (C) 2020 Intel Corporation.
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#  1. Redistributions of source code must retain the above copyright notice(s),
#     this list of conditions and the following disclaimer.
#  2. Redistributions in binary form must reproduce the above copyright notice(s),
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) ``AS IS'' AND ANY EXPRESS
#  OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
#  EVENT SHALL THE COPYRIGHT HOLDER(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
#  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
#  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
#  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
#  OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#
# run_local.sh - builds a container with Docker image
# and runs docker_run_test.sh script inside it
#
# Parameters:
# -name of the Docker image file

set -e

DOCKER_IMAGE_NAME="Dockerfile.fedora-31"

if [[ -z "$REDIS_HOST_PATH" ]]; then
    echo "REDIS_HOST_PATH has to be set."
    exit 1
fi

# Container path should be the same as WORKDIR in Dockerfile
REDIS_CONTAINER_PATH=/home/redisuser/redis/
CONTAINER_NAME=redis-6.0

docker build --tag "$CONTAINER_NAME" \
             --file "$DOCKER_IMAGE_NAME" \
             --build-arg http_proxy=$http_proxy \
             --build-arg https_proxy=$https_proxy \
             .

docker run --rm \
           --privileged=true \
           --tty=true \
           --env http_proxy=$http_proxy \
           --env https_proxy=$https_proxy \
           --env REDIS_CONTAINER_PATH="$REDIS_CONTAINER_PATH" \
           --env REDIS_ALLOCATOR="$REDIS_ALLOCATOR" \
           --mount type=bind,source="$REDIS_HOST_PATH",target="$REDIS_CONTAINER_PATH" \
            "$CONTAINER_NAME" utils/docker/docker_run_test.sh
