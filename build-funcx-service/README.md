# Build funcX Service
If you need to run the UniFaaS prototype, you need to build a special funcX service based on this repository.
It is important to note that UniFaaS is currently not compatible with the official funcX service (globus-compute).
The funcX service in this repository retains only the core functionalities of funcX, removing features such as authorization verification, user management, and persistent storage of functions.

## Prerequisites

- The funcX service needs to be deployed in a Kubernetes cluster. If you do not have a Kubernetes cluster, you can use [minikube](https://minikube.sigs.k8s.io/docs/start/) to set up a Kubernetes cluster on a single machine.
- Install kubectl. kubectl is the command line tool for Kubernetes, used for interacting with the Kubernetes cluster. Installation methods can be found on [Kubernetes](https://kubernetes.io/docs/tasks/tools/install-kubectl/).
- Install helm. Helm is the package manager for Kubernetes, used for deploying Kubernetes applications. Installation methods can be found on [Helm](https://helm.sh/docs/intro/install/).
- Check if kubectl and helm have been installed successfully.
```bash
kubectl version --client
helm version
```