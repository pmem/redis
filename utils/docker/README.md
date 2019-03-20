# **README**

This is utils/docker/README.

Dockerfile.fedora-29 is responsible for building a Docker container
with Fedora 29 environment and Redis installed


# Building the container

```
$ docker build . -f Dockerfile.fedora-29 -t redis_container \
 --build-arg http_proxy=$http_proxy \
 --build-arg https_proxy=$https_proxy
```

