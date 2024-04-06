# UniFaaS: Programming across Distributed Cyberinfrastructure with Federated Function Serving

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/SUSTech-HPCLab/UniFaaS/blob/main/LICENSE)

## What is UniFaaS?
UniFaas is a framework that adapts the convenient federated FaaS model to enable monolithic workflows to be broken into small schedulable function units that can be flexibly executed across a resilient, federated resource pool. 
You can execute the scientific workflows across distributed cyberinfrastructures with UniFaaS.
Here is the architecture of UniFaaS. See more details in our paper.

<p align="center">
  <img src="https://github.com/SUSTech-HPCLab/UniFaaS/blob/main/img/arch.png" width="50%"/>
</p>

**Currently, UniFaaS is still only a prototype. We will be dedicated to bug fixes and maintenance.**

## Declaration
**The execution backend of UniFaaS** is built upon funcx v0.3.4 but is **not compatible** with the official funcx version. If you wish to run the UniFaaS demo, you will need to follow the steps below to build a special version of the funcx web-service and special funcx endpoint. The client part of UniFaaS is based on Parsl v1.3.0, and we have only retained the necessary functionalities.

## Project Structure
- `build-funcx-service`: Since UniFaaS uses a modified version of the funcX service, if you want to run UniFaaS, you need to read the README.md in this folder and deploy a special funcX service in a Kubernetes cluster.
- `unifaas_endpoint`: The existing official funcX endpoints are not compatible with UniFaaS. You need to deploy the code from this folder to the machine where the endpoint will be installed.
- `unifaas`: The core code for UniFaaS, which is introduced in our paper. It can schedule and submit workflows to the endpoint.