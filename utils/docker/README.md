# **README**

This is utils/docker/README.

Scripts in this directory let build a Docker container with Fedora 29 environment 

*'docker_run_build_redis.sh'*  is used to build pmdk dependency and redis itself.

# Building the container

```
$ docker build . -f Dockerfile.fedora-29 -t redis_container \
 --build-arg http_proxy=$http_proxy \
 --build-arg https_proxy=$https_proxy 
```

