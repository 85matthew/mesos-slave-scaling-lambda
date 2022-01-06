# mesos-worker-scaling-lambda
AWS Lambda for safely scaling down Mesos worker on a Cloudwatch metric such as low CPU usage. Helps greatly to make sure you do not allow the AWS ASG to terminate a node randomly that may have a long running job on it.

Logic for lambda works likes this. 

1. Requires termination protection be enabled in AWS for your ASG
2. When a cloudwatch trigger fires for your desired scaldown event (low CPU for example), the ASG will show desired number of nodes are less than currently running.
3. Lambda will be triggered and will look for a Mesos worker that does not have any work.
4. When (or if) the Lambda finds a node that does not have work, it will drain the node so that no new work can be added to this host.
5. On the next lambda invocation, it will look for a host that are in 'drained' state and validate that no jobs are running on it.
6. If no jobs are running, the lambda will remove termination protection from the host and the ASG will terminate the host like normal


Environment Variables on Lambda

ENVIRONMENT= (staging, production, or any environment name)
APPLICATION= (only used for debugging logs)
ASG_NAME= (the name of the AWS ASG where you would like to safely scale down mesos worker)
