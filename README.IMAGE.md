Notes for Docker + Google Cloud

https://docs.docker.com/userguide/dockerizing/
https://cloud.google.com/compute/docs/containers/container_vms

# start docker
boot2docker start
$(boot2docker shellinit)

# build and run docker image locally
docker build -t gcr.io/twitter_for_bigquery/image ./image
docker run -i -t gcr.io/twitter_for_bigquery/image

# create instance
gcloud compute instances create exampleinstance \
    --scopes https://www.googleapis.com/auth/devstorage.read_write

# create instance with dockerized image
gcloud compute instances create examplecontainervm01 \
    --image container-vm \
    --metadata-from-file google-container-manifest=./container.yaml \
    --zone us-central1-b \
    --machine-type n1-highcpu-2
    
# zone    
us-central1-b

# log into an instance
gcloud compute instances list
gcloud compute --project "twitter-for-bigquery" ssh --zone "us-central1-b" "examplecontainervm01" 

# pull & run instance of image 
sudo docker pull gcr.io/twitter_for_bigquery/image
sudo docker run -d gcr.io/twitter_for_bigquery/image

# see logs
sudo -s
sudo docker ps
sudo docker logs --follow=true 5d
