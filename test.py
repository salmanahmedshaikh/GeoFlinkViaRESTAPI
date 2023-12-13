from FlinkRESTAPIMethods import *
import subprocess


def main():
    base_url = "http://localhost:8081/"
    # base_url = "http://localhost:29999/"
    # clusterDirectory = "/mnt/flink/flinkBinaries/flink-1.16.0/"

    jar_id = "iccaee477-8156-4c35-8d64-6c38110e3eb4_GeoFlinkProject-0.2.jar"  # (cluster)
    # jar_id = "d1381d82-c0d6-44e1-9723-f6976c7b4d55_GeoFlinkProject-0.2.jar"  #(local cluster)

    experimentFrequency = 2
    executionTimeSeconds = 120
    waitBetweenExecutionsSec = 30

    # 2101 TrajectorySimilarityQuery

    totalClusterTaskSlots = 30
    rangeQueryIDList = ["2101"]
    wIntervalList = [1000, 5000, 10000]
    wSlideStepList = [1, 15, 30]
    parallelismList = [10, 20, 30]
    # thresholdList = [0.00001, 0.00005, 0.0001]
    thresholdList = [0, 0.00005, 0.05]
    # thresholdList = [1, 5, 10]
    earlyAbandoningList = ["true"]
    numQueryTrajectoriesList = [100, 300, 500]
    # numQueryTrajectoriesList = [10, 20, 30, 40, 50]
    # numQueryTrajectoriesList = [20, 30, 40, 50]
    # algorithmList = ["DistributedNestedLoop", "MBRSlidingFullWindow", "NormalizedMBRSlidingFullWindow", "NormalizedMBRSlidingFullWindowOverlapping", "IncrementalMBR", "IncrementalNormalizedMBR", "IncrementalNormalizedMBROverlapping"]
    algorithmList = ["DistributedNestedLoop", "MBRSlidingFullWindow", "NormalizedMBRSlidingFullWindow",
                     "IncrementalMBR", "IncrementalNormalizedMBR"]
    # algorithmList = ["NormalizedMBRSlidingFullWindow",
    #                  "NormalizedMBRSlidingFullWindowOverlapping", "IncrementalMBR", "IncrementalNormalizedMBR",
    #                  "IncrementalNormalizedMBROverlapping"]
    # algorithmList = ["MBRSlidingFullWindow", "NormalizedMBRSlidingFullWindow",
    # "NormalizedMBRSlidingFullWindowOverlapping", "IncrementalMBR", "IncrementalNormalizedMBR",
    # "IncrementalNormalizedMBROverlapping"]
    inputTopicName = "NetworkMadagascar_Obj200_TI15_18M"
    outputTopicName = "NetworkMadagascar_Obj200_TI15_18M_Output"
    # queryTrajectoriesDirectory = "/mnt/flink/queryTrajectories/" #(cluster)
    # queryTrajectoriesDirectory = "/mnt/flink/SyntheticQueryTrajectories/"  # (cluster)
    queryTrajectoriesDirectory = "/mnt/flink/sortedNetworkMadagascar_Obj200_TI15_18M_QueryTraj_Short/"
    # queryTrajectoriesDirectory = "/data/NFS/shaikh/queryTrajectories/"  # (cluster)
    # queryTrajectoriesDirectory = "/data1/datasets/2D_Spatial/PortugalTaxiData_PKDD15/queryTrajectories/" #(local cluster)
    # bootStrapServers = "localhost:9092" #(local cluster)
    queryTrajectoriesFilesExtension = "geojson"
    bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092"  # (cluster)

    gridMinX = 0
    gridMinY = 0
    # dateFormat = "yyyy-MM-dd HH:mm:ss"
    dateFormat = "null"
    cellLength = 1
    gridRows = 98
    gridColumns = 100
    k = 1000
    queryTrajectorySlidePoints = 1

    subprocess.Popen("./../flinkBinaries/flink-1.16.0/bin/stop-cluster.sh", shell=True,
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

    # shutdownCluster(base_url)
    time.sleep(30)  # wait before starting cluster again

    outputFilePathAndName = "output/IncrTrajSimilarityExperiments_NetworkMadagascar_Obj200_TI15_18M_2.csv"
    logFilePathAndName = "logs/IncrTrajSimilarityExperiments_NetworkMadagascar_Obj200_TI15_18M_log.csv"

    # Default Values for Portugal Taxi Data
    # wInterval = 5000
    # wStep = 5
    # parallelism = 30
    # threshold = 0.00005
    # numQueryTrajectories = 300
    # earlyAbandoning = "true"

    for algorithm in algorithmList:
        for numQueryTrajectories in numQueryTrajectoriesList:
            # Default Values for Portugal Taxi Data
            queryID = "2101"
            wInterval = 5000
            wStep = 1
            parallelism = 30
            threshold = 0.00005
            earlyAbandoning = "true"

            executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep, dateFormat,
                                  gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                                  numQueryTrajectories, algorithm, earlyAbandoning, queryTrajectoriesDirectory,
                                  queryTrajectoriesFilesExtension, experimentFrequency,
                                  executionTimeSeconds, waitBetweenExecutionsSec, parallelism, base_url, jar_id,
                                  outputFilePathAndName, logFilePathAndName, bootStrapServers, totalClusterTaskSlots)

    for algorithm in algorithmList:
        for wInterval in wIntervalList:
            # Default Values for Portugal Taxi Data
            queryID = "2101"
            wStep = 5
            parallelism = 30
            threshold = 0.00005
            numQueryTrajectories = 300
            earlyAbandoning = "true"

            executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep,
                                  dateFormat,
                                  gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                                  numQueryTrajectories, algorithm, earlyAbandoning,
                                  queryTrajectoriesDirectory, queryTrajectoriesFilesExtension, experimentFrequency,
                                  executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                                  base_url, jar_id,
                                  outputFilePathAndName, logFilePathAndName, bootStrapServers, totalClusterTaskSlots)

    for algorithm in algorithmList:
        for wStep in wSlideStepList:
            # Default Values for Portugal Taxi Data
            queryID = "2101"
            wInterval = 5000
            parallelism = 30
            threshold = 0.00005
            numQueryTrajectories = 300
            earlyAbandoning = "true"

            executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep,
                                  dateFormat,
                                  gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                                  numQueryTrajectories, algorithm, earlyAbandoning,
                                  queryTrajectoriesDirectory, queryTrajectoriesFilesExtension, experimentFrequency,
                                  executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                                  base_url, jar_id,
                                  outputFilePathAndName, logFilePathAndName, bootStrapServers, totalClusterTaskSlots)

    for algorithm in algorithmList:
        for threshold in thresholdList:
            # Default Values for Portugal Taxi Data
            queryID = "2101"
            wInterval = 5000
            wStep = 5
            parallelism = 30
            numQueryTrajectories = 300
            earlyAbandoning = "true"

            executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep,
                                  dateFormat,
                                  gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                                  numQueryTrajectories, algorithm, earlyAbandoning,
                                  queryTrajectoriesDirectory, queryTrajectoriesFilesExtension, experimentFrequency,
                                  executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                                  base_url, jar_id,
                                  outputFilePathAndName, logFilePathAndName, bootStrapServers, totalClusterTaskSlots)

    for algorithm in algorithmList:
        for parallelism in parallelismList:
            # Default Values for Portugal Taxi Data
            queryID = "2101"
            wInterval = 5000
            wStep = 5
            threshold = 0.00005
            numQueryTrajectories = 300
            earlyAbandoning = "true"

            executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep,
                                  dateFormat,
                                  gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                                  numQueryTrajectories, algorithm, earlyAbandoning,
                                  queryTrajectoriesDirectory, queryTrajectoriesFilesExtension, experimentFrequency,
                                  executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                                  base_url, jar_id,
                                  outputFilePathAndName, logFilePathAndName, bootStrapServers, totalClusterTaskSlots)


def executeAndSaveLatency(queryID, inputTopicName, outputTopicName, k, wInterval, wStep, dateFormat,
                          gridMinX, gridMinY, cellLength, gridRows, gridColumns, threshold,
                          numQueryTrajectories, algorithm, earlyAbandoning, queryTrajectoriesDirectory,
                          queryTrajectoriesFilesExtension, experimentFrequency,
                          executionTimeSeconds, waitBetweenExecutionsSec, parallelism, base_url, jar_id,
                          outputFilePathAndName, logFilePathAndName, bootStrapServers, totalClusterTaskSlots):
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

        currentClusterTaskslots = 0

        # Checking if the complete cluster is started
        while (currentClusterTaskslots != totalClusterTaskSlots):
            # start cluster
            subprocess.Popen("./../flinkBinaries/flink-1.16.0/bin/start-cluster.sh", shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            # wait for starting the cluster
            time.sleep(30)
            currentClusterTaskslots = getFlinkClusterTotalSlots(base_url)

        # upload the jar after starting the cluster
        # uploadJar(base_url, "/data1/development/Flink/Projects/PrivateSpatialFlink/target/GeoFlinkProject-0.2.jar") # Ubuntu
        # uploadJar(base_url, "/Users/salman/Documents/Development/program_jars/GeoFlinkProject-0.2.jar")  # Mac
        uploadJar(base_url, "/mnt/flink/jars/GeoFlinkProject-0.2.jar")  # Cluster
        # wait for the jar to upload
        time.sleep(10)

        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']

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
                                          "-Dgeoflink.query.stateVarTTL=" + str(50),
                                          "-Dgeoflink.window.interval=" + str(wInterval),
                                          "-Dgeoflink.window.step=" + str(wStep),
                                          "-Dgeoflink.window.timeUnit=" + "millis",
                                          "-Dgeoflink.query.trajectorySimilarity.threshold=" + str(threshold),
                                          "-Dgeoflink.query.trajectorySimilarity.algorithm=" + algorithm,
                                          "-Dgeoflink.query.trajectorySimilarity.queryTrajectoriesDirectory=" + queryTrajectoriesDirectory,
                                          "-Dgeoflink.query.trajectorySimilarity.queryTrajectoriesFilesExtension=" + queryTrajectoriesFilesExtension,
                                          "-Dgeoflink.query.trajectorySimilarity.numQueryTrajectories=" + str(
                                              numQueryTrajectories),
                                          "-Dgeoflink.query.trajectorySimilarity.queryTrajectorySlidePoints=" + str(1),
                                          "-Dgeoflink.query.trajectorySimilarity.earlyAbandoning=" + earlyAbandoning
                                          ]}

        x = submitJob(base_url, jar_id, parameters)

        if x.status_code == 200:
            print(str(datetime.now()) + " *** Job submitted *** QueryID: " +
                  queryID + ", inputTopicName: " + inputTopicName + ", algorithm: " + algorithm + ", WinSize: " + str(
                wInterval) + ", WinStep: " + str(wStep) + ", Parallelism: " + str(parallelism) + ", Threshold: " + str(
                threshold)
                  + ", NumQueryTrajs: " + str(
                numQueryTrajectories) + ", EarlyAbandoning: " + earlyAbandoning + ", Frequency: " + str(i))
            logFile.write(str(datetime.now()) + " Job submitted: " +
                          queryID + "," + inputTopicName + "," + algorithm + "," + str(wInterval) + "," + str(
                wStep) + "," + str(parallelism) + "," + str(threshold)
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

        if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED" or int(actualExecutionDuration) < (
                executionTimeSeconds * 1000 - 1000):
            print("EXECUTION TIME INSUFFICIENT")
            time.sleep(waitBetweenExecutionsSec)
            subprocess.Popen("./../flinkBinaries/flink-1.16.0/bin/stop-cluster.sh",
                             shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            time.sleep(30)  # wait before starting cluster again
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
                print("STATE FAILED")
                time.sleep(waitBetweenExecutionsSec)
                subprocess.Popen("./../flinkBinaries/flink-1.16.0/bin/stop-cluster.sh",
                                 shell=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                time.sleep(30)  # wait before starting cluster again
                continue

        # wait at-least waitBetweenExecutionsSec seconds before starting next job
        # time.sleep(waitBetweenExecutionsSec)

        subprocess.Popen("./../flinkBinaries/flink-1.16.0/bin/stop-cluster.sh", shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        time.sleep(30)  # wait before starting cluster again

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
            vertexName = vertexName + str(vertexNameList[j].replace(',', '-')) + ", execDuration, " + execDuration + str(execDurationList[j]) + ", readRecords, " + readRecords + str(readRecordsList[j]) + ", writeRecords, " + writeRecords + str(writeRecordsList[j]) + ","
        else:
            vertexName = vertexName + str(vertexNameList[j].replace(',', '-')) + ", execDuration, " + execDuration + str(execDurationList[j]) + ", readRecords, " + readRecords + str(readRecordsList[j]) + ", writeRecords, " + writeRecords + str(writeRecordsList[j])

    vertexStr = vertexName

    # for j in range(len(vertexNameList)):
    #     if j != len(vertexNameList) - 1:
    #         vertexName = vertexName + str(vertexNameList[j].replace(',', '-')) + ","
    #         execDuration = execDuration + str(execDurationList[j]) + ","
    #         readRecords = readRecords + str(readRecordsList[j]) + ","
    #         writeRecords = writeRecords + str(writeRecordsList[j]) + ","
    #     else:
    #         vertexName = vertexName + str(vertexNameList[j].replace(',', '-'))
    #         execDuration = execDuration + str(execDurationList[j])
    #         readRecords = readRecords + str(readRecordsList[j])
    #         writeRecords = writeRecords + str(writeRecordsList[j])

    # vertexStr = vertexName + ", " + execDuration + ", " + readRecords + ", " + writeRecords

    statsFile.write(
        queryID + "," + inputTopicName + "," + algorithm + "," + str(wInterval) + "," + str(wStep) + "," + str(
            parallelism) + "," + str(threshold) + "," + str(
            numQueryTrajectories) + "," + earlyAbandoning + "," + vertexStr + "\n")
    print(queryID + "," + inputTopicName + "," + algorithm + "," + str(wInterval) + "," + str(wStep) + "," + str(
        parallelism) + "," + str(threshold) + "," + str(numQueryTrajectories) + "," + earlyAbandoning + "," + vertexStr)

    statsFile.flush()
    statsFile.close()
    logFile.flush()
    logFile.close()


if __name__ == "__main__":
    main()
