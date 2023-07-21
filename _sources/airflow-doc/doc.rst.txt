Restart Airflow
===============

# How to restart airflow scheduler and airflow webserver processes running in the Dev EC2 Linux instance and push them to background 

### Reboot the Dev EC2 instance from the AWS Management Console

![EC2 instance reboot](BIT_AGILISIUM/docs/images/snap-ec2-instance-reboot.png)

## Execute the following commands in sequence

```
$ airflow scheduler &
$ jobs
$ airflow webserver &
$ jobs
$ disown -h %1
$ jobs
$ disown -h %2
```

## If the latest changes in the DAGs are not reflected, try deleting the old log files

```
$ sudo rm airflow-scheduler.err  airflow-scheduler.pid airflow-scheduler.log airflow-webserver.err  airflow-webserver-monitor.pid airflow-webserver.log
```

## If you're facing issues logging into the default admin user post restart

Airflow v2.5 and above no longer create the default admin user with password. Which means even after the webserver is up and running you might still not be able to login to it using a previous credential or the standalone passwork you got standalone_admin_password.txt file in the airflow home folder.

Use the below command to create a new user with Admin privileges to solve this

```
$ airflow users create -e anand.thanumalayan@agilisium.com -f Anand -l Thanumalayan -p your_password -r Admin -u admin_anand
```