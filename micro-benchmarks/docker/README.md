# Docker README Usage

Guide goes through how to run experiment. Experiment runs in docker. It assumes that there are ample resources on the 
machine.

## Build the image

Ensure that the Dockerfile and setup-lib.sh script are in the directory.

```shell
docker build --no-cache -f Dockerfile -t jem-bench .
```

## Run the image
```shell
docker run -m 8g --cpus 4 jem-bench -Djvm.heap.size=4g
# Or, with jemalloc
docker run -m 8g --cpus 4 jem-bench -Djvm.heap.size=4g -Djemalloc.enabled=true
```