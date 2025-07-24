# eval $(minikube -p minikube docker-env)
eval $(minikube -p minikube docker-env)
docker build -f src/common/Dockerfile -t base:latest src/common
docker build -f src/KafkaAdminService/Dockerfile -t kafka-admin:latest src/KafkaAdminService
docker build -f src/FetcherAdminService/Dockerfile -t fetcher-admin:latest src/FetcherAdminService
docker build -f src/VKFetchingService/Dockerfile -t vk-fetcher:latest src/VKFetchingService
docker build -f src/botReader/Dockerfile -t bot-reader:latest src/botReader
docker build -f src/botWriter/Dockerfile -t bot-writer:latest src/botWriter
docker build -f src/ValidatorService/Dockerfile -t validator-service:latest src/ValidatorService
docker build -f src/flinkJobs/enrichposts/Dockerfile -t faust-enrichposts:latest src/flinkJobs/enrichposts