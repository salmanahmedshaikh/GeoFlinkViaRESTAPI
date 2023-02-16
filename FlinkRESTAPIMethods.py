import requests
import os
import json
import time
from datetime import datetime


def submitJob(base_url, jar_id, parameters):
    url = base_url + "jars/" + jar_id + "/run"
    myheader = {'content-type': 'application/json'}
    mydata = parameters

    x = requests.post(url, data=json.dumps(mydata), headers=myheader)
    return x


def terminateJob(base_url, job_id):
    url = base_url + "jobs/" + job_id
    mydata = {}
    x = requests.patch(url, data=json.dumps(mydata))
    return x


def uploadJar(base_url, path):
    url = base_url + "/jars/upload"
    myfile = {"jarfile": (
        os.path.basename(path),
        open(path, "rb"),
        "application/x-java-archive"
    )}
    x = requests.post(url, files=myfile)
    return x


def deleteJar(base_url, jar_id):
    url = base_url + "jars/" + jar_id
    mydata = '{}'
    x = requests.delete(url, data=mydata)
    return x


def getAllJobsOverview(base_url):
    url = base_url + "jobs/overview"
    mydata = '{}'
    x = requests.get(url, data=mydata)
    return x


def getJobOverview(base_url, job_id):
    url = base_url + "jobs/" + job_id
    mydata = '{}'
    x = requests.get(url, data=mydata)
    return x


def getAllJars(base_url):
    url = base_url + "jars"
    mydata = '{}'
    x = requests.get(url, data=mydata)
    return x


def getWebUIConfig(base_url):
    url = base_url + "config"
    mydata = '{}'
    x = requests.get(url, data=mydata)
    return x


def getFlinkClusterOverview(base_url):
    url = base_url + "overview"
    mydata = '{}'
    x = requests.get(url, data=mydata)
    return x


def openFile(filePathAndName):
    file = open(filePathAndName, 'a')
    file.truncate()
    file.close()
    file = open(filePathAndName, 'a')
    return file


def sum(data):
    total = 0
    for x in data:
        total += float(x)
    return total


def average(data):
    listSum = sum(data)
    average = listSum / float(len(data))
    return average


def variance(data):
    avg = average(data)
    variance = 0
    for x in data:
        variance += (avg - float(x)) ** 2
    return variance / len(data)


def std_deviation(data):
    return variance(data) ** 0.5


def avg_and_std_deviation(data):
    avg = average(data)
    std = std_deviation(data)
    return (avg, std)