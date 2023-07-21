Deploy Airflow
==============

# Steps to deploy DAG and code changes to Dev/UAT/Prod airflow EC2 instances

> Prerequisites
1. IP address of the respective dev/uat/prod EC2 instance
2. PEM key file to SSH into the EC2 instance

> Steps to just deploy latest code without restarting the Airflow scheduler and webserver
1. SSH into the respective EC2 instance
2. Navigate to src/github/igt/Bit_Agilisium folder
3. Do a git pull
4. Change to connection strings under ~/airflow/common/config.ini
5. Before running the pipeline, check if you have to truncate any of the existing records in landing, raw, cdm and features schemas
6. Goto the Airflow UI via the URL in the below format. Note: the https and the port 8080 are required
```
https://<EC2 instance IP address>:8080/home
```

> If you have to restart/start the Airflow scheduler and webserver

Refer the document "howto-restart-airflow-process.md"

# Steps to deploy schema changes to Dev/UAT/Prod Snowflake

1. Deploy schema changes
2. 

# Error while running the DAG from UI
If you're facing the below error, this could be because your SSH session has ended. While an active SSH session isn't required for a disowned process in linux, as a workaround try having the session open in the terminal 
```
Python version: 3.8.12
Airflow version: 2.5.0
Node: ip-10-0-0-8.us-west-2.compute.internal
-------------------------------------------------------------------------------
Traceback (most recent call last):
  File "/home/ec2-user/.local/lib/python3.8/site-packages/flask/app.py", line 2525, in wsgi_app
    response = self.full_dispatch_request()
  File "/home/ec2-user/.local/lib/python3.8/site-packages/flask/app.py", line 1822, in full_dispatch_request
    rv = self.handle_user_exception(e)
  File "/home/ec2-user/.local/lib/python3.8/site-packages/flask/app.py", line 1820, in full_dispatch_request
    rv = self.dispatch_request()
  File "/home/ec2-user/.local/lib/python3.8/site-packages/flask/app.py", line 1796, in dispatch_request
    return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)
  File "/home/ec2-user/.local/lib/python3.8/site-packages/airflow/www/auth.py", line 47, in decorated
    return func(*args, **kwargs)
  File "/home/ec2-user/.local/lib/python3.8/site-packages/airflow/www/decorators.py", line 125, in wrapper
    return f(*args, **kwargs)
  File "/home/ec2-user/.local/lib/python3.8/site-packages/airflow/utils/session.py", line 75, in wrapper
    return func(*args, session=session, **kwargs)
  File "/home/ec2-user/.local/lib/python3.8/site-packages/airflow/www/views.py", line 1982, in trigger
    if unpause and dag.is_paused:
  File "/home/ec2-user/.local/lib/python3.8/site-packages/airflow/models/dag.py", line 1277, in is_paused
    warnings.warn(
  File "/usr/lib64/python3.8/warnings.py", line 109, in _showwarnmsg
    sw(msg.message, msg.category, msg.filename, msg.lineno,
  File "/home/ec2-user/.local/lib/python3.8/site-packages/airflow/settings.py", line 126, in custom_show_warning
    write_console.print(msg, soft_wrap=True)
  File "/home/ec2-user/.local/lib/python3.8/site-packages/rich/console.py", line 1694, in print
    self._buffer.extend(new_segments)
  File "/home/ec2-user/.local/lib/python3.8/site-packages/rich/console.py", line 848, in __exit__
    self._exit_buffer()
  File "/home/ec2-user/.local/lib/python3.8/site-packages/rich/console.py", line 806, in _exit_buffer
    self._check_buffer()
  File "/home/ec2-user/.local/lib/python3.8/site-packages/rich/console.py", line 2016, in _check_buffer
    self.file.write(text)
OSError: [Errno 5] Input/output error
```