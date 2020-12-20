# Installing Apache Superset on Amazon EMR

## Overview

Add data exploration and visualization to your analytics cluster. Project files for the post, [Installing Apache Superset on Amazon EMR: Add data exploration and visualization to your analytics cluster](https://garystafford.medium.com/installing-apache-superset-on-amazon-emr-5e2444f6d242). Please see post for complete instructions on using the project's files.

### Create CloudFormation Stack

```shell script
python3 ./create_cfn_stack.py \
    --ec2-key-name <your_key_pair_name> \
    --ec2-subnet-id <your_subnet_id> \
    --environment dev
```

### Run Superset Bootstrap Script

```shell script
python3 ./install_superset.py \
    --ec2-key-path </path/to/my-key-pair.pem> \
    --superset-port 8280
```

### SSH Tunnel

Open an SSH tunnel to master node using dynamic port forwarding

```shell script
ssh -i </path/to/my-key-pair.pem> -ND 8157 hadoop@<public_master_dns>
```

### Troubleshooting Superset

Troubleshoot Superset process running on EMR Master.

```shell script
lsof -i :8280
```

## References

- https://superset.apache.org/docs/installation/installing-superset-from-scratch
- https://gitmemory.com/issue/apache/incubator-superset/8169/528679887
- https://stackoverflow.com/questions/59195394/apache-superset-config-py-on