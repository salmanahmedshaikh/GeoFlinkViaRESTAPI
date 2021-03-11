import requests
import os
import json
import time


def main():
    base_url = "http://localhost:29999/"

    # x = getAllJars(base_url)
    # x = getWebUIConfig(base_url)
    # x = uploadJar(base_url, path)
    # x = deleteJar(base_url, jar_id)

    jar_id = "55baf91f-11de-400d-95ba-84852d293c38_GeoFlinkProject20210309_2.jar"

    experimentFrequency = 3

    inputTopicNameList = ["TaxiDrive17MillionGeoJSON", "NYCBuildingsPolygons", "NYCBuildingsLineStrings"]
    ifApproximateQuery = ["true", "false"]
    radiusList = ["0.0005", "0.005", "0.05", "0.5"]
    wIntervalList = ["50", "100", "150", "200", "250"]
    wStepList = ["25", "50", "75", "100", "125"]
    uniformGridSizeList = ["100", "200", "300", "400", "500"]

    executionCostList = []
    numberRecordList = []

    queryOptionList = []
    dateFormat = ""
    gridMinX = ""
    gridMaxX = ""
    gridMinY = ""
    gridMaxY = ""
    trajIDSet = ""
    queryPoint = ""
    queryPolygon = ""
    queryLineString = ""

    for inputTopicName in inputTopicNameList:

        if inputTopicName == "TaxiDrive17MillionGeoJSON":
            queryOptionList = ["1", "6", "11"]
            dateFormat = "yyyy-MM-dd HH:mm:ss"
            gridMinX = "115.50000"
            gridMaxX = "117.60000"
            gridMinY = "39.60000"
            gridMaxY = "41.10000"
            trajIDSet = "9211800, 9320801, 9090500, 7282400, 10390100"
            queryPoint = "[116.14319183444924, 40.07271444145411]"
            queryPolygon = "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762], [116.14319183444924, 40.07271444145411]"
            queryLineString = "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762]"

        elif inputTopicName == "NYCBuildingsPolygons":
            queryOptionList = ["16", "21", "26"]
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            gridMinX = "-74.25540"
            gridMaxX = "-73.70007"
            gridMinY = "40.49843"
            gridMaxY = "40.91506"
            trajIDSet = "9211800, 9320801, 9090500, 7282400, 10390100"
            queryPoint = "[-74.0000, 40.72714]"
            queryPolygon = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]"
            queryLineString = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"

        else:
            queryOptionList = ["31", "36", "41"]
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            gridMinX = "-74.25540"
            gridMaxX = "-73.70007"
            gridMinY = "40.49843"
            gridMaxY = "40.91506"
            trajIDSet = "9211800, 9320801, 9090500, 7282400, 10390100"
            queryPoint = "[-74.0000, 40.72714]"
            queryPolygon = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]"
            queryLineString = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"

        for queryOption in queryOptionList:
            for approximateQuery in ifApproximateQuery:
                for radius in radiusList:
                    for wInterval in wIntervalList:
                        for wStep in wStepList:
                            for uniformGridSize in uniformGridSizeList:

                                executionCostList.clear()
                                numberRecordList.clear()

                                for i in range(experimentFrequency):
                                    parameters = {"programArgsList": ["--onCluster", "true",
                                                                      "--approximateQuery", approximateQuery,
                                                                      "--queryOption", queryOption,
                                                                      "--inputTopicName", inputTopicName,
                                                                      "--queryTopicName", "sampleTopic",
                                                                      "--outputTopicName", "QueryLatency",
                                                                      "--inputFormat", "GeoJSON",
                                                                      "--dateFormat", dateFormat,
                                                                      "--radius", radius,
                                                                      "--aggregate", "SUM",
                                                                      "--wType", "TIME",
                                                                      "--wInterval", wInterval,
                                                                      "--wStep", wStep,
                                                                      "--uniformGridSize", uniformGridSize,
                                                                      "--k", "10",
                                                                      "--trajDeletionThreshold", 1000,
                                                                      "--outOfOrderAllowedLateness", "1",
                                                                      "--omegaJoinDuration", "1",
                                                                      "--gridMinX", gridMinX,
                                                                      "--gridMaxX", gridMaxX,
                                                                      "--gridMinY", gridMinY,
                                                                      "--gridMaxY", gridMaxY,
                                                                      "--trajIDSet", trajIDSet,
                                                                      "--queryPoint", queryPoint,
                                                                      "--queryPolygon", queryPolygon,
                                                                      "--queryLineString", queryLineString],
                                                  "parallelism": 30}

                                    x = submitJob(base_url, jar_id, parameters)
                                    if x.status_code == 200:
                                        print("Job submitted: " +
                                              queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + wInterval + "," + wStep + "," + uniformGridSize)

                                    # Execute for 2 minutes
                                    time.sleep(60 * 2)

                                    job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
                                    y = getJobOverview(base_url, job_id)
                                    print(str(y.status_code) + ", " + y.text)

                                    while str(json.dumps(y.json()['vertices'][0]['metrics']['write-records-complete'], indent=4)) != "true":
                                        y = getJobOverview(base_url, job_id)
                                        print(str(y.status_code) + ", " + y.text)

                                    duration = json.dumps(y.json()['vertices'][0]['duration'], indent=4)
                                    print('duration : ' + duration)
                                    metrics = json.dumps(y.json()['vertices'], indent=4)

                                    records = json.dumps(y.json()['vertices'][0]['metrics']['write-records'], indent=4)
                                    print('records : ' + records)

                                    executionCostList.append(duration)
                                    numberRecordList.append(records)

                                    z = terminateJob(base_url, job_id)
                                    print(str(z.status_code) + ", " + z.text)

                                    # wait at-least 10 seconds before starting next job
                                    time.sleep(10)

                                file = openFile()

                                file.write(
                                    queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + wInterval + "," + wStep + "," + uniformGridSize + "," + str(
                                        executionCostList)[1:-1] + "," + str(numberRecordList)[1:-1] + "\n")
                                print(
                                    queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + wInterval + "," + wStep + "," + uniformGridSize + "," + str(
                                        executionCostList)[1:-1] + "," + str(numberRecordList)[1:-1])

                                file.flush()
                                file.close()

    # x = getAllJobsOverview(base_url)

    # y = json.loads(x.text)
    # for rows in y["jobs"]:
    #    if rows["state"] == "RUNNING":
    #        terminateJob(base_url, rows["jid"])


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


def openFile():
    file = open('qureyOutput.csv', 'a')
    file.truncate()
    file.close()
    file = open('qureyOutput.csv', 'a')
    return file


def grades_sum(data):
    total = 0
    for grade in data:
        total += grade
    return total


def grades_average(data):
    sum_of_grades = grades_sum(data)
    average = sum_of_grades / float(len(data))
    return average


def grades_variance(data):
    average = grades_average(data)
    variance = 0
    for score in data:
        variance += (average - score) ** 2
    return variance / len(data)


def grades_std_deviation(data):
    return grades_variance(data) ** 0.5


def calculate(data):
    average = grades_average(data)
    std = grades_std_deviation(data)
    return (average, std)


if __name__ == "__main__":
    main()
