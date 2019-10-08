from __future__ import print_function
import json
import logging
import requests
import boto3
import time
import os

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

PROTOCOL = 'http'
MESOS_MASTER_PORT = '5050'
MESOS_SLAVE_PORT = '64000'

# Establish boto3 client sessions
session = boto3.session.Session(region_name="us-east-1")
logger.debug("Session is in region %s ", session.region_name)
ec2Client = session.resource('ec2')
asgClient = session.client('autoscaling')

# Get env vars from Lambda
environment = os.environ['ENVIRONMENT']
application_name = os.environ['APPLICATION']
asg_name = os.environ['ASG_NAME']
logger.debug("environment:{}".format(environment))
logger.debug("application_name:{}".format(application_name))
logger.debug("asg_name:{}".format(asg_name))


def remove_termination_protection(instance_id):
    """
    Remove termination protection from a single instance
    """
    asgClient.set_instance_protection(
        InstanceIds=[instance_id], AutoScalingGroupName=asg_name, ProtectedFromScaleIn=False)


def need_scale_down(asgname):
    """
    Check if need to scale the ASG down (ie- are number nodes greater than desired nodes)
    """
    asg_settings = asgClient.describe_auto_scaling_groups(AutoScalingGroupNames=[asgname])
    num_instances_desired = asg_settings['AutoScalingGroups'][0]['DesiredCapacity']
    num_instances_running = len(asg_settings['AutoScalingGroups'][0]['Instances'])

    if num_instances_running > num_instances_desired:
        return True
    else:
        return False


def terminate_an_idle_worker(maintenance_url):
    """
    Look for a single worker that's idle and set it up for termination
    """
    # success == Did we successfully terminate an idle worker
    success = False

    asg_settings = asgClient.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    instances = asg_settings['AutoScalingGroups'][0]['Instances']

    for asg_instance in instances:
        logging.debug('Checking if we can decommision: {}'.format(asg_instance['InstanceId']))
        # Get an ec2 instance object
        instance = ec2Client.Instance(asg_instance['InstanceId'])

        if num_running_jobs(instance.private_ip_address) == 0:
            logging.debug('No jobs currently running for: {}'.format(instance.private_ip_address))
            if check_maintenance_enabled(maintenance_url, instance.private_ip_address):
                logging.debug('{} is already in the maintenance schedule'.format(instance.private_ip_address))
            else:
                logging.debug('Placing:{} in the maintenance schedule'.format(instance.private_ip_address))
                drain_node(maintenance_url, instance.private_ip_address)

                if check_maintenance_enabled(maintenance_url, instance.private_ip_address):
                    logging.debug('{} is now in the maintenance schedule'.format(instance.private_ip_address))
                else:
                    logging.debug('Cant add {} to maintenance schedule. EXITING'.format(instance.private_ip_address))
                    exit()

            if num_running_jobs(instance.private_ip_address) == 0:
                # Remove termination protection
                remove_termination_protection(instance.id)
                logging.debug('Remove termination protection on {}/{}'.format(instance.id, instance.private_ip_address))
                success = True
                break
            else:
                logging.debug('{} must have gotten work before we could decomm'.format(instance.private_ip_address))
        else:
            logging.debug('Tried terminating {}, but a job is running'.format(instance.private_ip_address))

    if success:
        return True
    else:
        return False


def check_maintenance_enabled(maintenance_url, host):
    """
    Check if maintenance is enabled for a host
    """
    req_state = requests.get(maintenance_url)
    if not req_state.ok:
        raise Exception(
            "Failed to get Maintenance Schedule:\n%s" % req_state.text
        )

    maintenance_schedule = json.loads(req_state.text)
    if len(maintenance_schedule) > 0:
        current_window = maintenance_schedule.get('windows', [])

        if 'machine_ids' in current_window[0]:
            for entry in maintenance_schedule['windows']:
                if entry['machine_ids'][0]['ip'] == host:
                    return True

    return False


def clean_maintenance_schedule(maintenance_url):
    """
    Remove nodes that are terminated from mesos maintenance schedule
    """
    logger.debug("Cleaning up maintenance schedule")
    req_state = requests.get(maintenance_url)
    maintenance = req_state.json()
    current_window = maintenance.get('windows', [])

    newwindow = []
    # Create new maintenance window and repost
    for window in current_window:
        t_info = window['unavailability']
        time_nano = time.time()*1000000000
        window_start = t_info["start"]["nanoseconds"]
        window_duration = t_info["duration"]["nanoseconds"]
        end_from_now = (window_start + window_duration) - time_nano

        # See if host is in 'Running' state, if not remove it from maintenance
        ip_address = window['machine_ids'][0]['ip']
        filter = [{'Name': 'private-ip-address', 'Values': [ip_address]}]

        # Create a client to find instance id by ip address
        ec2 = boto3.client('ec2')
        response = ec2.describe_instances(Filters=filter)

        # Check if the host is running
        if len(response['Reservations']) != 0:
            # host still exists, lets see if it's running
            logger.debug("Host state: {}".format(response['Reservations'][0]['Instances'][0]['State']['Name']))
            if response['Reservations'][0]['Instances'][0]['State']['Name'] != 'running':
                # host is not running
                logger.debug("Host is not in running state- removing from maintenance")
            else:
                logger.debug("Host still running, keeping in maintenance schedule")
                newwindow.append(window)
        else:
            logger.debug("{} is gone, removing from maintenance schedule".format(ip_address))
    logger.debug("Mesos Maintenance Cleanup Finished")

    if len(current_window) != len(newwindow):
        # Means the window changed and we should post it to mesos master
        logger.debug("Old Window " + str(maintenance))
        maintenance['windows'] = newwindow

        logger.debug("New Window " + str(maintenance))
        req_state = requests.post(
            maintenance_url,
            json=maintenance
        )
        logger.debug("Maintenance POST " + req_state.content)
        if not req_state.ok:
            raise Exception(
                "Failled to Set Maintenance (POST) mode to %s:\n%s" % (
                    maintenance_url, req_state.text
                )
            )


def drain_node(maintenance_url, host):
    """
    Set the Mesos node to draining so that it doesn't accept more work
    """
    logging.debug(host)
    if not host:
        logging.debug("unable to derive Mesos host from SNS message")
        return
    if check_maintenance_enabled(maintenance_url, host):
        logger.debug("Maintenance already enabled for: " + host)
    else:
        logger.debug("Adding host to maintenance for: " + host)

        req_state = requests.get(maintenance_url)
        maintenance = req_state.json()
        current_window = maintenance.get('windows', [])

        newwindow = []
        # Append node to maintenance and repost
        for window in current_window:
            t_info = window['unavailability']
            time_nano = time.time()*1000000000
            window_start = t_info["start"]["nanoseconds"]
            window_duration = t_info["duration"]["nanoseconds"]
            end_from_now = (window_start + window_duration) - time_nano
            if end_from_now > 0:
                newwindow.append(window)
            else:
                logging.debug(
                    "cleaning %s maintenance ended since %d s",
                    window["machine_ids"],
                    end_from_now/1000000000
                )
        # maintenance from now + 20s  for 18 hrs (our est max for a job to finish) (64800000000000)
        newwindow.append({
            "machine_ids": [{"ip": host, "hostname": host}],
            "unavailability": {
                "start": {
                    "nanoseconds": int(time.time()*1000000000 + 20000000000)
                },
                # 64800000000000 = 18 hours (our longest running job)
                "duration": {"nanoseconds": 64800000000000}
            }
        })
        logger.debug("Old Window " + str(maintenance))
        maintenance['windows'] = newwindow

        logger.debug("New Window " + str(maintenance))
        req_state = requests.post(
            maintenance_url,
            json=maintenance
        )

        logger.debug("Maintenance POST " + req_state.content)
        logging.debug("req_state" + str(req_state.content))
        if not req_state.ok:
            raise Exception(
                "Failed to Set Maintenance (POST) mode to %s:\n%s" % (
                    maintenance_url, req_state.text
                )
            )
    return


def num_running_jobs(host):
    """
    Queries the slave to get the number of actively running tasks
    """
    req_state = requests.get(
        "{}://{}:{}/monitor/statistics".format
        (PROTOCOL, host, MESOS_SLAVE_PORT)
    )

    logger.debug("NUM JOBS RUNNING:" + req_state.text)

    if not req_state.ok:
        raise Exception(
            "Failled to get number running jobs for: %s:\n%s" % (
                host, req_state.text
            )
        )
    return len(req_state.json())


def instance_alive(instance):
    """
    This function is to test if the aws instance object is still running.
    If it's not running for some amount of time,
    methods will return an exception which breaks things
    """
    try:
        logger.debug("Instance state: " + str(instance.state))
        if instance.state['Name'] == 'running':
            return True
        else:
            logger.debug("Instance Not running")
            return False
    except AttributeError:
        logger.debug("Instance is not running any more")
        return False


def lambda_handler(event, context):

    if environment == "staging":
        # TODO: Move to an env variable
        mesos_master_host = 'mesosmaster.staging.mydomain.com'
    else:
        # TODO: Move to an env variable
        mesos_master_host = 'mesosmaster.prod.mydomain.com'
    maintenance_url = '{}://{}:{}/master/maintenance/schedule'.format(
        PROTOCOL, mesos_master_host, MESOS_MASTER_PORT
    )

    if need_scale_down(asg_name):
        logger.debug("{} needs to be scaled down. Running...".format(asg_name))
        # Find a host to decomm
        if terminate_an_idle_worker(maintenance_url):
            logger.debug("Successfully found an idle worker")
        else:
            logger.debug("Didn't find an idle worker to terminate")
    else:
        logger.debug("No work to do")

    logger.debug("Running Mesos Maintenance Schedule Cleanup")
    clean_maintenance_schedule(maintenance_url)
