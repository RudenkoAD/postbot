#windows powershell/cmd only
# minikube -p minikube docker-env | Invoke-Expression
#bash only
eval $(minikube -p minikube docker-env)
#both
# helm uninstall postbot
./build.sh
helm upgrade postbot ./helm