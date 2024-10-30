minikube start --driver=docker
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml
# У меня работало только с VPN
helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version 0.16.2