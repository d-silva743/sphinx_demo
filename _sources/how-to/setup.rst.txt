Setup your environment
======================

IDE
---

- Visual Studio Code
    - [Installation URL](https://code.visualstudio.com/)
    - Install the required extensions for Python, Git etc.,

Anaconda
--------
- Anaconda setup with Python 3.8 and Snowpark
    - [Installation URL](https://www.anaconda.com/products/distribution)
    - Create new conda virtual environment with Python 3.8 and Snowpark 1.0 using dependencies file. 
    - Please use the yaml dependency file **'venv_conda_snowpark.yaml'** available in this folder. Feel free to change the environment name in the dependencies file if needed.
        ```
        $ conda env create -f venv_conda_snowpark.yaml
        ```
    - Activate conda environment and check Python and Snowpark versions
        ```
        $ conda activate virtual_env_name
        $ python --version
        $ pip --version
        ```
    - If Pip is not installed
        ```
        $ python3 -m pip install --upgrade pip
        ```
Airflow
-------
- Airflow quick installation inside conda venv
    - Activate conda environment created in the previous step, ignore if activated already
        ```
        $ conda activate virtual_env_name
        ```
    - Follow the steps mentioned in the [official quickstart URL](https://airflow.apache.org/docs/apache-airflow/stable/start.html) to install a standalone version of the latest airflow, intended for development purposes. 
    - Note: Airflow does not officially support Windows OS, you may need to enable WSL or use Docker for running Airflow dags. You can still run the `.py` files directly for troubleshooting purposes
    - Alternatively you can use the below command to quickly install version 2.5.0 of Airflow
        ```
        $ pip install "apache-airflow==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.8.txt"
        ```
    - All airflow commands must be executed from within the Conda virtual environment e.g., `$ airflow standalone` or `$ airflow version` etc.,

Pip packages installation
-------------------------
- Post successful Airflow installation via pip you can install the other pip packages like Snowpark, Pandas etc., required for compiling the files in the project
- Open terminal and `'cd'` your way into the `'docs\environment\dev'` folder, where the `requirements.txt` reside.
    ```
    $ pip install -r requirements.txt
    ```
> **Troubleshooting**
- If you face any error like 'No matching distribution found for snowflake-snowpark-python', check the pip version to see if its mapped to the right python version. It should not be mapped to site-packages folder from other python versions like `/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages/pip (python 3.10)`
    ```
    $ pip --version
    ```
- If you face any dependency resolution related errors in the above step, try
    ```
    $ pip install -r requirements.txt --use-deprecated=legacy-resolver
    ```


Git installation
----------------
- Git setup - Mac OS
    - In terminal, execute the following command and follow the steps in the resulting installation wizard
        ```
        $ xcode-select --install
        ```

- Git setup - Windows
    - [Git for Windows installation URL](https://git-scm.com/download/win)

Gitlab
------
- Get the login credentials with the appropriate contributor permissions

Git configuration and clone
---------------------------
- In the right source code folder
    ```
    $ git config --global credential.helper store
    $ git config --global user.name "FIRST_NAME LAST_NAME"
    $ git config --global user.email 'emailId'
    $ git clone https://gitlab.nbcc.mobi/bit-agilisium/bit_agilisium.git
    ```

PostgreSQL
----------
- PostgreSQL for Mac
    - Install Homebrew [URL](https://brew.sh/) (if needed in localhost)
    - In terminal,
        ```
        $ brew install postgresql
        $ brew services list
        $ brew services restart postgresql
        $ createuser -s postgres
        ```
    - Install PgAdmin 4 - [URL](https://www.pgadmin.org/download/)