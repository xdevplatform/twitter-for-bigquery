Notes for Docker + Google Cloud

https://docs.docker.com/userguide/dockerizing/
https://cloud.google.com/compute/docs/containers/container_vms

# start docker
boot2docker start
$(boot2docker shellinit)

# build and run docker image locally
docker build -t gcr.io/twitter_for_bigquery/image_twitter ./image_twitter
docker run -i -t gcr.io/twitter_for_bigquery/image_twitter

# push to container registry
gcloud preview docker push gcr.io/twitter_for_bigquery/image_twitter

# create instance
gcloud compute instances create exampleinstance \
    --scopes https://www.googleapis.com/auth/devstorage.read_write

# load vm... something something?
gcloud compute instances create examplecontainervm01 \
    --image container-vm \
    --metadata-from-file google-container-manifest=./image_twitter/container.yaml \
    --zone us-central1-b \
    --machine-type f1-micro
    
# zone    
us-central1-b

# log into an instance
gcloud compute instances list
gcloud compute --project "twitter-for-bigquery" ssh --zone "us-central1-b" "examplecontainervm01" 

# run instance of image 
docker run gcr.io/twitter_for_bigquery/image_twitter

# see logs
sudo -s
sudo docker ps
sudo docker logs --follow=true 5d


---

gcloud compute instances create containervm-test-1 \
    --image container-vm \
    --metadata-from-file google-container-manifest=./image_twitter/container_test.yaml \
    --zone us-central1-b \
    --machine-type f1-micro

gcloud config set container/cluster cluster-1
gcloud config set compute/zone us-central1-b
gcloud alpha container kubectl run-container twitter-pod \
    --image gcr.io/twitter_for_bigquery/image_twitter
    
gcloud alpha container kubectl run-container --cluster="cluster-1" twitter-pod \
    --image gcr.io/twitter_for_bigquery/image_twitter
