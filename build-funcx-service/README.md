# Build funcX Service
If you need to run the UniFaaS prototype, you need to build a special funcX service based on this repository.
It is important to note that **UniFaaS is currently not compatible with the official funcX service (globus-compute).**
**The funcX service in this repository retains only the core functionalities of funcX, removing features such as authorization verification, user management, and persistent storage of functions, etc.**

## Prerequisites

- The funcX service needs to be deployed in a Kubernetes cluster. If you do not have a Kubernetes cluster, you can use [minikube](https://minikube.sigs.k8s.io/docs/start/) to set up a Kubernetes cluster on a single machine.
- Install kubectl. kubectl is the command line tool for Kubernetes, used for interacting with the Kubernetes cluster. Installation methods can be found on [Kubernetes](https://kubernetes.io/docs/tasks/tools/install-kubectl/).
- Install helm. Helm is the package manager for Kubernetes, used for deploying Kubernetes applications. Installation methods can be found on [Helm](https://helm.sh/docs/intro/install/).
- Check if kubectl and helm have been installed successfully.
```bash
kubectl version --client
helm version
```

## Deploying funcX Service
1. Modify `advertise_ip: << input your advertised ip here >>` in `./deployed_values/values.yaml` to an accessible IP address or domain name. For instance, if your endpoints are distributed across different networks, you need to provide a public IP address for the k8s cluster. If the endpoint and funcX service are on the same network, you can use an accessible LAN IP address.
2. Execute the following command in the current directory (`./unifaas/build-funcx-service`) to deploy the funcX service.
```bash
helm install -f deployed_values/values.yaml funcx funcx
```
3. After a successful deployment, you can check the status of the funcX service with the following command.
```bash
kubectl get pods
```
If you see the following output, it means that the funcX service has been successfully deployed.
```bash
NAME                                             READY   STATUS    RESTARTS   AGE
funcx-forwarder-7b5b8879b5-sk7qn                 1/1     Running   0          51m
funcx-funcx-web-service-68f6dd95df-bdsl8         1/1     Running   0          51m
funcx-funcx-websocket-service-78fc655df6-2t66k   1/1     Running   0          51m
funcx-postgresql-0                               1/1     Running   0          51m
funcx-rabbitmq-0                                 1/1     Running   0          51m
funcx-redis-master-0                             1/1     Running   0          51m
funcx-redis-slave-0                              1/1     Running   0          51m
funcx-redis-slave-1                              1/1     Running   0          51m
```
4. Since a ClusterIP type of Service is used by default, we need to expose the service to the local environment by using port-forwarding. Run the following command:
```bash
./forward.sh
```
This script will expose the funcX service on port 5000 of 0.0.0.0. You can check if the service is running normally by `curl http://ip:5000/v2/version`.
**Note: This kind of deployment method is only suitable for a test environment. If you want to deploy the funcX service on a public cloud service, please modify the values to a LoadBalancer type of Service.**

5. If you want to remove the funcX service, you can run the following command:
```bash
helm uninstall funcx
./kill_forward.sh  # This will kill all the processes of kubectl port-forward
```

## Acknowledgement
This repository is modified from the archived [funcX helm-chart](https://github.com/funcx-faas/helm-chart). Thanks to the funcX team.
 
