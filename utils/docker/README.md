# **README**

This is utils/docker/README.

Scripts in this directory let build a Docker container with Fedora 29 environment 

Container is endowed with two versions of Redis:
*redis-server* is pmem version, based on memkind allocator
*redis-server-original* is the original Redis, from https://github.com/antirez/redis

# Building the container

```
$ docker build . -f Dockerfile.fedora-29 -t redis_container \
 --build-arg http_proxy=$http_proxy \
 --build-arg https_proxy=$https_proxy
```

