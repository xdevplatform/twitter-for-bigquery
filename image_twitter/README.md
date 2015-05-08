Notes for Docker + Google Cloud

https://docs.docker.com/userguide/dockerizing/
https://cloud.google.com/compute/docs/containers/container_vms

# start docker
boot2docker start
$(boot2docker shellinit)

# build and run docker image locally
docker build -t image_twitter ./image_twitter
docker run -i -t image_twitter

# push docker image to gcloud
gcloud compute instances create image_twitter_1 \
    --image image_twitter \
    --metadata-from-file google-container-manifest=image_twitter.yaml \
    --zone us-central1-a \
    --machine-type f1-micro