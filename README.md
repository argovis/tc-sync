This repo contains scripts and data for populating tropical cyclone information in Argovis.

## Rebuilding from scratch

 - Make sure you've created empty collections for tropical cyclone data in MongoDB via the `tc.py` script in [https://github.com/argovis/db-schema](https://github.com/argovis/db-schema).
 - Build the image described in `Dockerfile`; run a pod or container based on it in the appropriate kube namespace or docker container network to connect to your MongoDB container, mount storage to capture the logs at `/tmp/tc` in the container, and in that container run `bash parseall.sh`. TC data will be quickly repopulated in MongoDB.
 - [optional] Note the `roundtrip` scripts for doing a proofreading pass of everything that got written to MongoDB.