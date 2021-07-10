#!/bin/bash

# install python packages on all nodes
sudo apt-get update
sudo apt-get install -y python-pip
sudo pip install pipenv    