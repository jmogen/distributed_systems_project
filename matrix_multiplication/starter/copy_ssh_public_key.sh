#!/bin/bash

# Copy SSH public key to cluster nodes for passwordless authentication
# Replace node addresses in cluster_hosts file with your actual nodes

ssh-keyscan -H $(cat cluster_hosts) >> ~/.ssh/known_hosts

for host in $(cat cluster_hosts); do 
	echo $host
	ssh-copy-id -i ~/.ssh/id_rsa.pub $host
done
