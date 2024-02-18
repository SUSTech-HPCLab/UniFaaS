# UniFaaS: Programming across Distributed Cyberinfrastructure with Federated Function Serving

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
