from FlinkRESTAPIMethods import *


def main():
    base_url = "http://localhost:29999/"
    x = getAllJars(base_url)

    jar_id = "36381fe0-4cb1-469f-8b70-a08e82d0ae53_GeoFlinkProject-0.2.jar"

    experimentFrequency = 5
    executionTimeSeconds = 120
    waitBetweenExecutionsSec = 90

    # 2041 TrajectoryPolygonTRangeQuery
    # 2042 IncrementalTumblingWinBasedTRangeQuery
    # 2043 IncrementalTumblingNoGridTRangeQuery
    # 2044 IncrementalTumblingLazyTRangeQuery
    # 2045 IncrementalTumblingPanesPartialsTRangeQuery
    # 2046 IncrementalTumblingPanesPartialsLazyTRangeQuery

    rangeQueryIDList = ["2041", "2042", "2043", "2044", "2045", "2046"]
    wIntervalList = [5, 50, 100, 150, 200]

    inputTopicName = "SpatialTrajs1000IDs100Million"
    outputTopicName = ""
    parallelism = 30

    gridMinX = 0
    gridMinY = 0
    dateFormat = "yyyy-MM-dd HH:mm:ss"
    cellLength = 1
    gridRows = 100
    gridColumns = 100
    k = 1000
    wInterval = 5
    wStep = 5

    outputFilePathAndName = "output/TStreamIncrementalQueryExperiments.csv"
    logFilePathAndName = "logs/TStreamIncrementalQueryExperiments_log.csv"

    for wInterval in wIntervalList:
        for queryID in rangeQueryIDList:
            executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep, dateFormat,
                                  gridMinX, gridMinY, cellLength, gridRows, gridColumns, experimentFrequency,
                                  executionTimeSeconds, waitBetweenExecutionsSec, parallelism, base_url, jar_id,
                                  outputFilePathAndName, logFilePathAndName)


def executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep, dateFormat,
                          gridMinX, gridMinY, cellLength, gridRows, gridColumns, experimentFrequency,
                          executionTimeSeconds, waitBetweenExecutionsSec, parallelism, base_url, jar_id,
                          outputFilePathAndName, logFilePathAndName):
    bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092"

    executionCostList = []
    numberRecordList = []
    vertexNameList = []
    execDurationList = []
    readRecordsList = []
    writeRecordsList = []

    executionCostList.clear()
    numberRecordList.clear()

    logFile = openFile(logFilePathAndName)

    #for i in range(experimentFrequency):
    i = 0
    while i < experimentFrequency:
        parameters = {"programArgsList": ["-Dgeoflink.clusterMode=true",
                                          "-Dgeoflink.kafkaBootStrapServers=" + bootStrapServers,
                                          "-Dgeoflink.parallelism=" + str(parallelism),
                                          "-Dgeoflink.inputStream1.topicName=" + inputTopicName,
                                          "-Dgeoflink.inputStream1.minX=" + str(gridMinX),
                                          "-Dgeoflink.inputStream1.minY=" + str(gridMinY),
                                          "-Dgeoflink.inputStream1.dateFormat=" + dateFormat,
                                          "-Dgeoflink.inputStream1.cellLength=" + str(cellLength),
                                          "-Dgeoflink.inputStream1.gridRows=" + str(gridRows),
                                          "-Dgeoflink.inputStream1.gridColumns=" + str(gridColumns),
                                          "-Dgeoflink.query.option=" + queryID,
                                          "-Dgeoflink.query.k=" + str(k),
                                          "-Dgeoflink.window.interval=" + str(wInterval),
                                          "-Dgeoflink.window.step=" + str(wStep)
                                          ]}

        x = submitJob(base_url, jar_id, parameters)

        if x.status_code == 200:
            print(str(datetime.now()) + " Job submitted: " +
                  queryID + "," + inputTopicName + "," + str(k) + "," + str(cellLength) + "," + str(
                gridRows) + "," + str(gridColumns) + "," + str(wInterval) + "," + str(wStep) + ", Frequency " + str(i))
            logFile.write(str(datetime.now()) + " Job submitted: " +
                          queryID + "," + inputTopicName + "," + str(k) + "," + str(cellLength) + "," + str(
                gridRows) + "," + str(gridColumns) + "," + str(wInterval) + "," + str(wStep) + ", Frequency " + str(
                i) + "\n")
        else:
            print(str(datetime.now()) + " Job could not be submitted: " + x.text)
            logFile.write(str(datetime.now()) + "Job could not be submitted: " + x.text + "\n")

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        print(str(datetime.now()) + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + str(y.status_code) + ", " + y.text + "\n")

        while str(json.dumps(y.json()['vertices'][0]['metrics']['write-records-complete'], indent=4)) != "true":
            time.sleep(1)
            y = getJobOverview(base_url, job_id)
            print(str(datetime.now()) + str(y.status_code) + ", " + y.text)
            logFile.write(str(datetime.now()) + str(y.status_code) + ", " + y.text + "\n")

        jsonTxt = json.loads(y.text)

        z = terminateJob(base_url, job_id)
        print(str(datetime.now()) + str(z.status_code) + ", " + z.text)
        logFile.write(str(datetime.now()) + str(z.status_code) + ", " + z.text + "\n")

        actualExecutionDuration = 0
        for vertex in jsonTxt["vertices"]:
            actualExecutionDuration = json.dumps(vertex['duration'], indent=4)

        if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED" or int(actualExecutionDuration) < (executionTimeSeconds * 1000):
            time.sleep(waitBetweenExecutionsSec)
            continue

        for vertex in jsonTxt["vertices"]:
            vertexNameList.append(json.dumps(vertex['name'], indent=4))
            execDurationList.append(json.dumps(vertex['duration'], indent=4))
            readRecordsList.append(json.dumps(vertex['metrics']['read-records'], indent=4))
            writeRecordsList.append(json.dumps(vertex['metrics']['write-records'], indent=4))

        while str(json.dumps(y.json()['state'], indent=4)).strip('\"') != "CANCELED":
            time.sleep(10)
            terminateJob(base_url, job_id)
            y = getJobOverview(base_url, job_id)
            print(str(datetime.now()) + str(y.status_code) + ", " + y.text)
            logFile.write(str(datetime.now()) + str(y.status_code) + ", " + y.text + "\n")
            print(str(datetime.now()) + str(json.dumps(y.json()['state'], indent=4)))
            logFile.write(str(datetime.now()) + str(json.dumps(y.json()['state'], indent=4)) + "\n")
            if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED":
                break

        # wait at-least waitBetweenExecutionsSec seconds before starting next job
        time.sleep(waitBetweenExecutionsSec)

        # incrementing loop variable
        i += 1

    statsFile = openFile(outputFilePathAndName)

    # vertexStr = ""
    # for j in range(len(vertexNameList)):
    #     vertexStr = vertexStr + ", " + vertexNameList[j].replace(',', '-') + ", " + execDurationList[j] + ", " + \s
    #                 readRecordsList[j] + ", " + writeRecordsList[j]

    execDuration = ""
    readRecords = ""
    writeRecords = ""
    vertexName = ""

    for j in range(len(vertexNameList)):
        if j != len(vertexNameList) - 1:
            vertexName = vertexName + str(vertexNameList[j].replace(',', '-')) + ","
            execDuration = execDuration + str(execDurationList[j]) + ","
            readRecords = readRecords + str(readRecordsList[j]) + ","
            writeRecords = writeRecords + str(writeRecordsList[j]) + ","
        else:
            vertexName = vertexName + str(vertexNameList[j].replace(',', '-'))
            execDuration = execDuration + str(execDurationList[j])
            readRecords = readRecords + str(readRecordsList[j])
            writeRecords = writeRecords + str(writeRecordsList[j])

    vertexStr = vertexName + ", " + execDuration + ", " + readRecords + ", " + writeRecords

    statsFile.write(
        queryID + "," + inputTopicName + "," + str(k) + "," + str(cellLength) + "," + str(gridRows) + "," + str(
            gridColumns) + "," + str(wInterval) + "," + str(wStep) + "," + vertexStr + "\n")
    print(queryID + "," + inputTopicName + "," + str(k) + "," + str(cellLength) + "," + str(gridRows) + "," + str(
        gridColumns) + "," + str(wInterval) + "," + str(wStep) + "," + vertexStr)

    statsFile.flush()
    statsFile.close()
    logFile.flush()

    logFile.close()


if __name__ == "__main__":
    main()
