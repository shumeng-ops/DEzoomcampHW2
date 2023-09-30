from prefect.deployments import Deployment
from etl_hw2 import etl_parent_flow
from prefect.filesystems import GitHub

github_block = GitHub.load("github-hw2")

github_block.get_directory("etl_hw2") # specify a subfolder of repo
github_block.save("dev")




