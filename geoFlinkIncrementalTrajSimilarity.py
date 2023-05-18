from FlinkRESTAPIMethods import *


def main():
    base_url = "http://localhost:8081/"
    x = getAllJars(base_url)

    jar_id = "92598495-1dbe-43ea-abb8-1608ecc30456_GeoFlinkProject-0.2.jar"

    experimentFrequency = 3
    executionTimeSeconds = 10
    waitBetweenExecutionsSec = 10

    # 2101 TrajectorySimilarityQuery

    rangeQueryIDList = ["2101"]
    wIntervalList = [2000, 4000, 6000, 8000, 10000]
    wSlideStepList = [1, 5, 10, 15, 20, 25]
    parallelismList = [1, 10, 20, 30]
    thresholdList = [0.00001, 0.00005, 0.0001]
    earlyAbandoningList = ["true"]
    numQueryTrajectoriesList = [100, 200, 300, 400, 500]
    algorithmList = ["DistributedNestedLoop", "MBRSlidingFullWindow", "NormalizedMBRSlidingFullWindow", "IncrementalMBR", "IncrementalNormalizedMBR"]
    inputTopicName = "PortugalTaxiTrajs"
    outputTopicName = "PortugalTaxiTrajOutput"
    bootStrapServers = "localhost:9092"

    gridMinX = 0
    gridMinY = 0
    dateFormat = "yyyy-MM-dd HH:mm:ss"
    cellLength = 1
    gridRows = 100
    gridColumns = 100
    k = 1000

    outputFilePathAndName = "output/TStreamIncrementalTrajSimilarityExperiments.csv"
    logFilePathAndName = "logs/TStreamIncrementalTrajSimilarityExperiments_log.csv"

    for queryID in rangeQueryIDList:
        for earlyAbandoning in earlyAbandoningList:
            for wInterval in wIntervalList:
                for wStep in wSlideStepList:
                    for parallelism in parallelismList:
                        for threshold in thresholdList:
                            for numQueryTrajectories in numQueryTrajectoriesList:
                                for algorithm in algorithmList:

                                    executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep, dateFormat,
                                                          gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                                                          numQueryTrajectories, algorithm, earlyAbandoning, experimentFrequency,
                                                          executionTimeSeconds, waitBetweenExecutionsSec, parallelism, base_url, jar_id,
                                                          outputFilePathAndName, logFilePathAndName, bootStrapServers)


def executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep, dateFormat,
                                                          gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                                                          numQueryTrajectories, algorithm, earlyAbandoning, experimentFrequency,
                                                          executionTimeSeconds, waitBetweenExecutionsSec, parallelism, base_url, jar_id,
                                                          outputFilePathAndName, logFilePathAndName, bootStrapServers):

    executionCostList = []
    numberRecordList = []
    vertexNameList = []
    execDurationList = []
    readRecordsList = []
    writeRecordsList = []

    executionCostList.clear()
    numberRecordList.clear()

    logFile = openFile(logFilePathAndName)

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
                                          "-Dgeoflink.window.step=" + str(wStep),
                                          "-Dgeoflink.trajectorySimilarity.threshold=" + str(threshold),
                                          "-Dgeoflink.trajectorySimilarity.algorithm=" + algorithm,
                                          "-Dgeoflink.trajectorySimilarity.numQueryTrajectories=" + str(numQueryTrajectories),
                                          "-Dgeoflink.trajectorySimilarity.earlyAbandoning=" + earlyAbandoning
                                          ]}

        x = submitJob(base_url, jar_id, parameters)

        if x.status_code == 200:
            print(str(datetime.now()) + " Job submitted: " +
                  queryID + "," + inputTopicName + "," + algorithm + "," + str(wInterval) + "," + str(wStep) + "," + str(parallelism) + "," + str(threshold)
                  + "," + str(numQueryTrajectories) + "," + earlyAbandoning + ", Frequency " + str(i))
            logFile.write(str(datetime.now()) + " Job submitted: " +
                  queryID + "," + inputTopicName + "," + algorithm + "," + str(wInterval) + "," + str(wStep) + "," + str(parallelism) + "," + str(threshold)
                  + "," + str(numQueryTrajectories) + "," + earlyAbandoning + ", Frequency " + str(i) + "\n")
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
        queryID + "," + inputTopicName + "," + algorithm + "," + str(wInterval) + "," + str(wStep) + "," + str(
            parallelism) + "," + str(threshold) + "," + str(numQueryTrajectories) + "," + earlyAbandoning + "," + vertexStr + "\n")
    print(queryID + "," + inputTopicName + "," + algorithm + "," + str(wInterval) + "," + str(wStep) + "," + str(
            parallelism) + "," + str(threshold) + "," + str(numQueryTrajectories) + "," + earlyAbandoning + "," + vertexStr)

    statsFile.flush()
    statsFile.close()
    logFile.flush()

    logFile.close()


if __name__ == "__main__":
    main()
