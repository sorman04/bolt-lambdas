These are the basic instructions to deploy a docker container to a lambda function.

The workflow is as follows:

1. create the python module you want to deploy as lambda function. the module will have the code implemented as a def called handler
2. create the requirements.txt file to list all the dependencies needed by the handler function
3. create the Dockerfile using the template provided
4. build the image using docker build --platform linux/amd64 -t image_name .
5. check that the container works as intended by running the container with the following instructions:
   a. docker run --platform linux/amd64 -p 9000:8080 image_name
   b. use powershell to input: Invoke-WebRequest -Uri "http://localhost:9000/2015-03-31/functions/function/invocations" -Method Post -Body '{}' -ContentType "application/json"
6. authenticate the docker cli to ECR by using aws cli command: aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 111122223333.dkr.ecr.us-east-1.amazonaws.com
7. create an ECR repository to store the image with aws cli command (or from console): aws ecr create-repository --repository-name image_name --region us-east-1 --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
8. tag the docker image using ecr repository uri by doing: docker tag image_name ecr-repo-uri/image_name:latest
9. push the image to ECR: docker push ecr-repo-uri/image_name:latest
10. create the lambda function by creating a cloud formation stack using the yaml template
