# ReadMe
This is a project to release self designed Ansible modules. 

# How to initialize development environment
Install Python version 2.7.12, and run `pip install -r requirements.txt` to install necessary packages.

# How to use
1. Please copy **emr/lib/aws_emr.py** to your ansible lib path or set **emr/lib** as your ansible lib path. Default config file is under **/etc/ansible/ansible.cfg**. 

2. Please generate **emr/examples/main.yml** file based on the template file **main.yml.template**. You need to set AWS Access Key, AWS Security Key and AWS Region based on your account. You might also need to add VPC information as well.  

3. After all settings, you can try to manage EMR cluster by provide example playbooks. 
