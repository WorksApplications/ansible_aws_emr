# ReadMe
This is a project to release self designed Ansible modules. 

# Environment Info
ansible version: 2.5.5
Python  version: 2.7.12
pip version: 9.0.1 
boto: 2.48.0
boto3: 1.7.22

# How to use
1. Please copy **emr/lib/aws_emr.py** to your ansible lib path or set **emr/lib** as your ansible lib path. Default config file is under **/etc/ansible/ansible.cfg**. 

2. Please generate **emr/examples/main.yml** file based on the template file **main.yml.template**. You need to set AWS Access Key, AWS Security Key and AWS Region based on your account. You might also need to add VPC information as well.  

3. After all settings, you can try to manage EMR cluster by provide example playbooks. 
