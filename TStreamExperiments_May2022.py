import requests
import os
import json
import time
from datetime import datetime


def main():
    base_url = "http://localhost:29999/"
    x = getAllJars(base_url)
    # x = getWebUIConfig(base_url)
    # x = uploadJar(base_url, path)
    # x = deleteJar(base_url, jar_id)

    jar_id = "f58d4555-13a4-45fd-b022-b7ba386abfa8_GeoFlinkProject-0.1.jar"

    experimentFrequency = 2
    executionTimeSeconds = 120
    waitBetweenExecutionsSec = 60

    inputTopicNameList = ["TaxiDrive", "ATCShoppingMall"]
    wIntervalList = ["10", "20", "30", "40", "50"]
    wStepList = ["5", "10", "15", "20", "25"]
    uniformGridSizeList = ["100", "200", "300", "400", "500"]

    queryOptionListNaive = []
    queryOptionListGridBased = []
    dateFormat = ""
    gridMinX = ""
    gridMaxX = ""
    gridMinY = ""
    gridMaxY = ""
    trajIDSet = ""
    queryPoint = ""
    queryPolygon = ""
    queryLineString = ""

    #file = openFile(outputFilePathAndName)
    #file.write("queryOption" + "," + "approximateQuery" + "," + "inputTopicName" + "," + "radius" + "," + "wInterval" + "," + "wStep" + "," + "uniformGridSize" + "," + "executionCost1, executionCost2, executionCost3" + "," + "avg_time_sec" + ", " + "numberRecords1, numberRecords2, numberRecords3, " + "avg_records" + ", " + "throughput" + "\n")
    #file.flush()
    #file.close()


    # Join Query
    '''
    for inputTopicName in inputTopicNameList:

        outputFilePathAndName = "qureyOutput_TStream_JoinQuery.csv"
        logFilePathAndName = "TStreamQueryLogJoin.csv"
        radiusList = []

        approximateQuery = "false"
        k = "10"  # k in join query indicates query stream rate

        if inputTopicName == "TaxiDrive":
            queryOptionListNaive = ["2100"]
            queryOptionListGridBased = ["210"]
            radiusList = ["0.0005", "0.005", "0.05", "0.5"]
            radius = "0.005"
            queryTopicName = "TaxiDrive_Live"
            dateFormat = "yyyy-MM-dd HH:mm:ss"
            gridMinX = "115.50000"
            gridMaxX = "117.60000"
            gridMinY = "39.60000"
            gridMaxY = "41.10000"

        elif inputTopicName == "ATCShoppingMall":
            queryOptionListNaive = ["2100"]
            queryOptionListGridBased = ["210"]
            radiusList = ["5", "50", "500", "5000"]
            radius = "50"
            queryTopicName = "ATCShoppingMall_Live"
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            gridMinX = "-41543.0"
            gridMaxX = "48431.0"
            gridMinY = "-27825.0"
            gridMaxY = "24224.0"

        # Variables for variable stream rate experiments
        wInterval = "30"
        wStep = "15"
        uniformGridSize = "200"
        numQueryPoints = "100" # numQueryPoints in join query indicates number of distinct objects in query stream

        for queryOption in queryOptionListNaive:

            executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                   wInterval, wStep,
                                   uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   numQueryPoints,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName, logFilePathAndName)
            
            for radius2 in radiusList:
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                numQueryPoints = "300"
                k = "10"  # k in join query indicates query stream rate
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius2, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

        for queryOption in queryOptionListGridBased:

            executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                   wInterval, wStep,
                                   uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   numQueryPoints,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName, logFilePathAndName)
            
            for radius2 in radiusList:
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                numQueryPoints = "300"
                k = "10"  # k in join query indicates query stream rate
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius2, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)
                                        
            for uniformGridSize in uniformGridSizeList:
                wInterval = "30"
                wStep = "15"
                numQueryPoints = "300"
                k = "10"  # k in join query indicates query stream rate
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)
                
                
    # Range Query with multiple polygons
    inputTopicNameList = ["ATCShoppingMall"]
    for inputTopicName in inputTopicNameList:

        outputFilePathAndName = "qureyOutput_TStream_RangeQuery_2.csv"
        logFilePathAndName = "TStreamQueryLogRange.csv"
        kList = ["500", "1000", "1500", "2000"]  # k in range query indicates number of polygons

        approximateQuery = "false"
        numQueryPoints = str(0)
        queryTopicName = ""

        if inputTopicName == "TaxiDrive":
            queryOptionListNaive = ["2040"]
            queryOptionListGridBased = ["204"]
            dateFormat = "yyyy-MM-dd HH:mm:ss"
            gridMinX = "115.50000"
            gridMaxX = "117.60000"
            gridMinY = "39.60000"
            gridMaxY = "41.10000"

        elif inputTopicName == "ATCShoppingMall":
            queryOptionListNaive = ["2040"]
            queryOptionListGridBased = ["204"]
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            gridMinX = "-41543.0"
            gridMaxX = "48431.0"
            gridMinY = "-27825.0"
            gridMaxY = "24224.0"

        
        for queryOption in queryOptionListNaive:

            for k in kList:
                radius = "0"
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

            for wInterval in wIntervalList:
                radius = "0"
                wStep = "15"
                uniformGridSize = "200"
                k = "1000"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

            for wStep in wStepList:
                radius = "0"
                wInterval = "30"
                uniformGridSize = "200"
                k = "1000"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName) 

        for queryOption in queryOptionListGridBased:            
            for k in kList:
                radius = "0"
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

            for wInterval in wIntervalList:
                radius = "0"
                wStep = "15"
                uniformGridSize = "200"
                k = "1000"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

            for wStep in wStepList:
                radius = "0"
                wInterval = "30"
                uniformGridSize = "200"
                k = "1000"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)
                                       

            uniformGridSizeList = ["500"]
            for uniformGridSize in uniformGridSizeList:
                radius = "0"
                wInterval = "30"
                wStep = "15"
                k = "1000"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k,
                                       wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                       numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)
                                       '''


    # Knn Query with multiple query points
    for inputTopicName in inputTopicNameList:

        outputFilePathAndName = "qureyOutput_TStream_kNNQuery.csv"
        logFilePathAndName = "TStreamQueryLogKnn.csv"
        radiusList = []
        kList = ["20", "40", "60", "80", "100"] # number of k
        numQueryPointsList = ["100", "200", "300", "400", "500"] # number of query points

        approximateQuery = "false"
        queryTopicName = ""

        if inputTopicName == "TaxiDrive":
            queryOptionListNaive = ["2130"]
            queryOptionListGridBased = ["213"]
            radiusList = ["0.0005", "0.005", "0.05", "0.5"]
            radius = "0.005"
            dateFormat = "yyyy-MM-dd HH:mm:ss"
            gridMinX = "115.50000"
            gridMaxX = "117.60000"
            gridMinY = "39.60000"
            gridMaxY = "41.10000"

        elif inputTopicName == "ATCShoppingMall":
            queryOptionListNaive = ["2130"]
            queryOptionListGridBased = ["213"]
            radiusList = ["5", "50", "500", "5000"]
            radius = "50"
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            gridMinX = "-41543.0"
            gridMaxX = "48431.0"
            gridMinY = "-27825.0"
            gridMaxY = "24224.0"

        for queryOption in queryOptionListNaive:
            '''
            for k in kList:
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                numQueryPoints = "300"                
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

            for radius2 in radiusList:
                k = "40"
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                numQueryPoints = "300"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius2, k, wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)
                                       '''

            for numQueryPoints in numQueryPointsList:
                k = "40"
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

        for queryOption in queryOptionListGridBased:
            '''
            for k in kList:
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                numQueryPoints = "300"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

            for radius2 in radiusList:
                k = "40"
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                numQueryPoints = "300"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius2, k, wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)
                                       '''

            for numQueryPoints in numQueryPointsList:
                k = "40"
                wInterval = "30"
                wStep = "15"
                uniformGridSize = "200"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)

            '''
            for uniformGridSize in uniformGridSizeList:
                k = "40"
                wInterval = "30"
                wStep = "15"
                numQueryPoints = "300"
                executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                                       uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints,
                                       trajIDSet, queryPoint, queryPolygon, queryLineString,
                                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                       base_url, jar_id, outputFilePathAndName, logFilePathAndName)
                                       '''

def executeAndSaveDetailed(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                           uniformGridSize, dateFormat,
                           gridMinX, gridMaxX, gridMinY, gridMaxY, numQueryPoints, trajIDSet, queryPoint, queryPolygon,
                           queryLineString,
                           experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, base_url, jar_id,
                           outputFilePathAndName, logFilePathAndName):

    cellLength = abs((float(gridMaxX) - float(gridMinX))) / float(uniformGridSize)
    logFile = openFile(logFilePathAndName)

    for i in range(experimentFrequency):
        vertexNameList = []
        execDurationList = []
        readRecordsList = []
        writeRecordsList = []
        parameters = {"programArgsList": ["-Dgeoflink.clusterMode=true",
                                          "-Dgeoflink.parallelism=30",
                                          "-Dgeoflink.inputStream1.topicName=" + inputTopicName,
                                          "-Dgeoflink.inputStream1.minX=" + gridMinX,
                                          "-Dgeoflink.inputStream1.minY=" + gridMinY,
                                          "-Dgeoflink.inputStream1.dateFormat=" + dateFormat,
                                          "-Dgeoflink.inputStream1.cellLength=" + str(cellLength),
                                          "-Dgeoflink.inputStream1.gridRows=" + uniformGridSize,
                                          "-Dgeoflink.inputStream1.gridColumns=" + uniformGridSize,
                                          "-Dgeoflink.inputStream2.topicName=" + queryTopicName,
                                          "-Dgeoflink.inputStream2.minX=" + gridMinX,
                                          "-Dgeoflink.inputStream2.minY=" + gridMinY,
                                          "-Dgeoflink.inputStream2.dateFormat=yyyy-MM-dd HH:mm:ss",
                                          "-Dgeoflink.inputStream2.cellLength=" + str(cellLength),
                                          "-Dgeoflink.inputStream2.gridRows=" + uniformGridSize,
                                          "-Dgeoflink.inputStream2.gridColumns=" + uniformGridSize,
                                          "-Dgeoflink.query.option=" + queryOption,
                                          "-Dgeoflink.query.k=" + k,
                                          "-Dgeoflink.query.radius=" + radius,
                                          "-Dgeoflink.query.omegaDuration=" + numQueryPoints,
                                          "-Dgeoflink.window.interval=" + wInterval,
                                          "-Dgeoflink.window.step=" + wStep
                                          ]}

        x = submitJob(base_url, jar_id, parameters)
        if x.status_code == 200:
            print(str(datetime.now()) + " Job submitted: " +
                  queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + k + "," + numQueryPoints + "," + wInterval + "," + wStep + "," + uniformGridSize+ ", Frequency " + str(i))
            logFile.write(str(datetime.now()) + "Job submitted: " +
                  queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + k + "," + numQueryPoints + "," + wInterval + "," + wStep + "," + uniformGridSize+ ", Frequency " + str(i)+ "\n")
        else:
            print(str(datetime.now()) + " Job could not be submitted: " + x.text)
            logFile.write(str(datetime.now()) + "Job could not be submitted: " + x.text+ "\n")

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        print(str(datetime.now()) + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + str(y.status_code) + ", " + y.text+ "\n")

        while str(json.dumps(y.json()['vertices'][0]['metrics']['write-records-complete'], indent=4)) != "true":
            time.sleep(1)
            y = getJobOverview(base_url, job_id)
            print(str(datetime.now()) + str(y.status_code) + ", " + y.text)
            logFile.write(str(datetime.now()) + str(y.status_code) + ", " + y.text+ "\n")

        jsonTxt = json.loads(y.text)

        for vertex in jsonTxt["vertices"]:
            vertexNameList.append(json.dumps(vertex['name'], indent=4))
            execDurationList.append(json.dumps(vertex['duration'], indent=4))
            readRecordsList.append(json.dumps(vertex['metrics']['read-records'], indent=4))
            writeRecordsList.append(json.dumps(vertex['metrics']['write-records'], indent=4))

        z = terminateJob(base_url, job_id)
        print(str(datetime.now()) + str(z.status_code) + ", " + z.text)
        logFile.write(str(datetime.now()) + str(z.status_code) + ", " + z.text+ "\n")

        y = getJobOverview(base_url, job_id)
        print(str(datetime.now()) + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + str(y.status_code) + ", " + y.text+ "\n")

        while str(json.dumps(y.json()['state'], indent=4)).strip('\"') != "CANCELED":
            time.sleep(10)
            terminateJob(base_url, job_id)
            y = getJobOverview(base_url, job_id)
            print(str(datetime.now()) + str(y.status_code) + ", " + y.text)
            logFile.write(str(datetime.now()) + str(y.status_code) + ", " + y.text+ "\n")
            print(str(datetime.now()) + str(json.dumps(y.json()['state'], indent=4)))
            logFile.write(str(datetime.now()) + str(json.dumps(y.json()['state'], indent=4))+ "\n")
            if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED":
                break

        # wait at-least 10 seconds before starting next job
        time.sleep(waitBetweenExecutionsSec)

        file = openFile(outputFilePathAndName)

        vertexStr = ""
        for j in range(len(vertexNameList)):
            vertexStr = vertexStr + ", " + vertexNameList[j].replace(',', '-') + ", " + execDurationList[j] + ", " + \
                        readRecordsList[
                            j] + ", " + writeRecordsList[j]

        file.write(
            queryOption + "," + str(i) + "," + inputTopicName + "," + radius + "," + k + "," + numQueryPoints + "," + wInterval + "," + wStep + "," + uniformGridSize + vertexStr + "\n")
        print(
            queryOption + "," + str(i) + "," + inputTopicName + "," + radius + "," + k + "," + numQueryPoints + "," + wInterval + "," + wStep + "," + uniformGridSize + vertexStr)

        file.flush()
        file.close()
        logFile.flush()

    logFile.close()


    '''
    # Join Query
    outputFilePathAndName = "qureyOutput_JoinQuery.csv"
    inputTopicNameList = ["TaxiDrive17MillionGeoJSON", "NYCBuildingsPolygons", "NYCBuildingsLineStrings"]
    queryTopicNameList = ["TaxiDriveGeoJSON_Live", "NYCBuildingsPolygonsGeoJSON_Live",
                          "NYCBuildingsLineStringsGeoJSON_Live"]
    # radiusList = ["0.0005", "0.005", "0.05", "0.5"]
    radiusList = ["0.0005", "0.005", "0.05"]
    wIntervalList = ["50", "100", "150", "200", "250"]
    wStepList = ["25", "50", "75", "100", "125"]
    uniformGridSizeList = ["100", "200", "300", "400", "500"]

    queryOptionListWindowed = []
    queryOptionListRealtime = []
    dateFormat = ""
    gridMinX = ""
    gridMaxX = ""
    gridMinY = ""
    gridMaxY = ""
    qGridMinX = ""
    qGridMaxX = ""
    qGridMinY = ""
    qGridMaxY = ""
    trajIDSet = ""
    queryPoint = ""
    queryPolygon = ""
    queryLineString = ""
    queryDateFormat = ""
    queryTopicName = ""


    file = openFile(outputFilePathAndName)
    file.write(
        "queryOption" + "," + "approximateQuery" + "," + "inputTopicName" + "," + "radius" + "," + "k" + "," + "wInterval" + "," + "wStep" + "," + "queryStreamRate" + "," + "uniformGridSize" + "," + "executionCost1, executionCost2, executionCost3" + "," + "avg_time_sec" + ", " + "numberRecords1, numberRecords2, numberRecords3, " + "avg_records" + ", " + "throughput" + "\n")
    file.flush()
    file.close()
    '''

    '''
    for inputTopicName in inputTopicNameList:
        if inputTopicName == "TaxiDrive17MillionGeoJSON":
            queryOptionListWindowed = ["101", "106", "111"]
            queryOptionListRealtime = ["102", "107", "112"]
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
            queryOptionListWindowed = ["116", "121", "126"]
            queryOptionListRealtime = ["117", "122", "127"]
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
            queryOptionListWindowed = ["131", "136", "141"]
            queryOptionListRealtime = ["132", "137", "142"]
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            gridMinX = "-74.25540"
            gridMaxX = "-73.70007"
            gridMinY = "40.49843"
            gridMaxY = "40.91506"
            trajIDSet = "9211800, 9320801, 9090500, 7282400, 10390100"
            queryPoint = "[-74.0000, 40.72714]"
            queryPolygon = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]"
            queryLineString = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"

        for queryOption in queryOptionListWindowed:
            if queryOption == "101" or queryOption == "102":
                queryTopicName = "TaxiDriveGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "116" or queryOption == "117" or queryOption == "131" or queryOption == "132":
                queryTopicName = "NYC_Points_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "106" or queryOption == "107":
                queryTopicName = "Beijing_Polygons_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "121" or queryOption == "122" or queryOption == "136" or queryOption == "137":
                queryTopicName = "NYCBuildingsPolygonsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "111" or queryOption == "112":
                queryTopicName = "Beijing_LineStrings_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            else:
                queryTopicName = "NYCBuildingsLineStringsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"

            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                queryStreamRate = "10"  # tuples/second
                executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                                   wStep, queryStreamRate,
                                   uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName)

            for wInterval in wIntervalList:
                approximateQuery = "true"
                radius = "0.005"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                queryStreamRate = "10"
                executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                                   wStep, queryStreamRate,
                                   uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName)

            for wStep in wStepList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                uniformGridSize = "200"
                k = "50"
                queryStreamRate = "10"
                executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                                   wStep, queryStreamRate,
                                   uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                k = "50"
                queryStreamRate = "10"
                executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                                   wStep, queryStreamRate,
                                   uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName)

        for queryOption in queryOptionListRealtime:
            if queryOption == "101" or queryOption == "102":
                queryTopicName = "TaxiDriveGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "116" or queryOption == "117" or queryOption == "131" or queryOption == "132":
                queryTopicName = "NYC_Points_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "106" or queryOption == "107":
                queryTopicName = "Beijing_Polygons_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "121" or queryOption == "122" or queryOption == "136" or queryOption == "137":
                queryTopicName = "NYCBuildingsPolygonsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "111" or queryOption == "112":
                queryTopicName = "Beijing_LineStrings_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            else:
                queryTopicName = "NYCBuildingsLineStringsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"

            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                queryStreamRate = "10"
                executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                                   wStep, queryStreamRate,
                                   uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                k = "50"
                queryStreamRate = "10"
                executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                                   wStep, queryStreamRate,
                                   uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                                   qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                                   trajIDSet, queryPoint, queryPolygon, queryLineString,
                                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                                   base_url, jar_id, outputFilePathAndName)
    '''

    '''
    for inputTopicName in inputTopicNameList:
        if inputTopicName == "TaxiDrive17MillionGeoJSON":
            queryOptionListWindowed = ["101", "106", "111"]
            queryOptionListRealtime = ["102", "107", "112"]
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
            queryOptionListWindowed = ["116", "121", "126"]
            queryOptionListRealtime = ["117", "122", "127"]
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
            queryOptionListWindowed = ["131", "136", "141"]
            queryOptionListRealtime = ["132", "137", "142"]
            dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            gridMinX = "-74.25540"
            gridMaxX = "-73.70007"
            gridMinY = "40.49843"
            gridMaxY = "40.91506"
            trajIDSet = "9211800, 9320801, 9090500, 7282400, 10390100"
            queryPoint = "[-74.0000, 40.72714]"
            queryPolygon = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]"
            queryLineString = "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"

        for queryOption in queryOptionListWindowed:
            if queryOption == "101" or queryOption == "102":
                queryTopicName = "TaxiDriveGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "116" or queryOption == "117" or queryOption == "131" or queryOption == "132":
                queryTopicName = "NYC_Points_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "106" or queryOption == "107":
                queryTopicName = "Beijing_Polygons_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "121" or queryOption == "122" or queryOption == "136" or queryOption == "137":
                queryTopicName = "NYCBuildingsPolygonsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "111" or queryOption == "112":
                queryTopicName = "Beijing_LineStrings_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            else:
                queryTopicName = "NYCBuildingsLineStringsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"

            approximateQuery = "true"
            wInterval = "100"
            wStep = "50"
            uniformGridSize = "200"
            k = "50"
            radius = "0.005"
            queryStreamRate = "0.01"  # tuples/second
            executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                               wStep, queryStreamRate,
                               uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

        for queryOption in queryOptionListRealtime:
            if queryOption == "101" or queryOption == "102":
                queryTopicName = "TaxiDriveGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "116" or queryOption == "117" or queryOption == "131" or queryOption == "132":
                queryTopicName = "NYC_Points_Live"
                queryDateFormat = "yyyy-MM-dd HH:mm:ss"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "106" or queryOption == "107":
                queryTopicName = "Beijing_Polygons_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            elif queryOption == "121" or queryOption == "122" or queryOption == "136" or queryOption == "137":
                queryTopicName = "NYCBuildingsPolygonsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"
            elif queryOption == "111" or queryOption == "112":
                queryTopicName = "Beijing_LineStrings_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "115.50000"
                qGridMaxX = "117.60000"
                qGridMinY = "39.60000"
                qGridMaxY = "41.10000"
            else:
                queryTopicName = "NYCBuildingsLineStringsGeoJSON_Live"
                queryDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                qGridMinX = "-74.25540"
                qGridMaxX = "-73.70007"
                qGridMinY = "40.49843"
                qGridMaxY = "40.91506"

            approximateQuery = "true"
            wInterval = "100"
            wStep = "50"
            uniformGridSize = "200"
            k = "50"
            radius = "0.005"
            queryStreamRate = "0.01"
            executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                               wStep, queryStreamRate,
                               uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)                               
       '''

    # Latency Experiments
    '''
    #queryOptionList = ["8", "9", "58", "59", "108", "109"]
    queryOptionList = ["8", "58", "108"]  #window based
    outputFilePathAndName = "queryLatency.csv"

    for queryOption in queryOptionList:
        inputTopicName = "TaxiDrive17MillionGeoJSON"
        dateFormat = "yyyy-MM-dd HH:mm:ss"
        gridMinX = "115.50000"
        gridMaxX = "117.60000"
        gridMinY = "39.60000"
        gridMaxY = "41.10000"
        trajIDSet = "9211800, 9320801, 9090500, 7282400, 10390100"
        queryPoint = "[116.14319183444924, 40.07271444145411]"
        queryPolygon = "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762], [116.14319183444924, 40.07271444145411]"
        queryLineString = "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762]"
        queryTopicName = "Beijing_Polygons_Live"
        queryDateFormat = "yyyy-MM-dd HH:mm:ss"
        qGridMinX = "115.50000"
        qGridMaxX = "117.60000"
        qGridMinY = "39.60000"
        qGridMaxY = "41.10000"
        approximateQuery = "true"
        wInterval = "10"
        wStep = "5"
        uniformGridSize = "200"
        k = "50"
        radius = "0.005"
        queryStreamRate = "0.1"  # tuples/second
        outputTopicName = "latency" + queryOption + "_wSize" + wInterval + "_sStep" + wStep
        executeAndSaveLatency(queryOption, approximateQuery, inputTopicName, queryTopicName, outputTopicName, radius, k, wInterval,
                           wStep, queryStreamRate,
                           uniformGridSize, dateFormat, queryDateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                           qGridMinX, qGridMaxX, qGridMinY, qGridMaxY,
                           trajIDSet, queryPoint, queryPolygon, queryLineString,
                           experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                           base_url, jar_id, outputFilePathAndName)
       '''


def executeAndSaveLatency(queryOption, approximateQuery, inputTopicName, queryTopicName, outputTopicName, radius, k,
                          wInterval, wStep,
                          queryStreamRate, uniformGridSize, dateFormat, queryDateFormat,
                          gridMinX, gridMaxX, gridMinY, gridMaxY, qGridMinX, qGridMaxX, qGridMinY, qGridMaxY, trajIDSet,
                          queryPoint, queryPolygon, queryLineString,
                          experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, base_url, jar_id,
                          outputFilePathAndName):
    executionCostList = []
    numberRecordList = []
    vertexNameList = []
    execDurationList = []
    readRecordsList = []
    writeRecordsList = []

    executionCostList.clear()
    numberRecordList.clear()

    cellLength = (gridMaxX - gridMinX)/uniformGridSize

    for i in range(experimentFrequency):
        parameters = {"programArgsList": ["-Dgeoflink.clusterMode = true",
                                          "-Dgeoflink.parallelism = 30",
                                          "-Dgeoflink.inputStream1.topicName = ", inputTopicName,
                                          "-Dgeoflink.inputStream2.topicName = ", queryTopicName,
                                          "-Dgeoflink.inputStream1.minX = ", gridMinX,
                                          "-Dgeoflink.inputStream1.minY = ", gridMinY,
                                          "-Dgeoflink.inputStream1.dateFormat = ", dateFormat,
                                          "-Dgeoflink.inputStream1.cellLength = ", cellLength,
                                          "-Dgeoflink.inputStream1.gridRows = ", uniformGridSize,
                                          "-Dgeoflink.inputStream1.gridColumns = ", uniformGridSize,
                                          "-Dgeoflink.query.option = ", queryOption,
                                          "-Dgeoflink.query.k = ", k,
                                          "-Dgeoflink.query.radius = ", radius,
                                          "-Dgeoflink.query.omegaDuration = 100",
                                          "-Dgeoflink.window.interval = ", wInterval,
                                          "-Dgeoflink.window.step = ", wStep],
                      "parallelism": 30}

        x = submitJob(base_url, jar_id, parameters)
        if x.status_code == 200:
            print("Job submitted: " +
                  queryOption + "," + approximateQuery + "," + inputTopicName + "," + queryTopicName + "," + outputTopicName + "," + queryDateFormat + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + uniformGridSize)
        else:
            print("Job could not be submitted: " + x.text)

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        print(str(y.status_code) + ", " + y.text)

        while str(json.dumps(y.json()['vertices'][0]['metrics']['write-records-complete'], indent=4)) != "true":
            time.sleep(1)
            y = getJobOverview(base_url, job_id)
            print(str(y.status_code) + ", " + y.text)

        jsonTxt = json.loads(y.text)

        duration = 0
        records = -999999
        for vertex in jsonTxt["vertices"]:
            vertexNameList.append(json.dumps(vertex['name'], indent=4))
            execDurationList.append(json.dumps(vertex['duration'], indent=4))
            readRecordsList.append(json.dumps(vertex['metrics']['read-records'], indent=4))
            writeRecordsList.append(json.dumps(vertex['metrics']['write-records'], indent=4))
            # print(json.dumps(vertex['duration'], indent=4))
            # print(json.dumps(vertex['metrics']['write-records'], indent=4))
            # name = json.dumps(vertex['name'], indent=4)
            # duration = json.dumps(vertex['duration'], indent=4)
            # read_records = int(json.dumps(vertex['metrics']['read-records'], indent=4))
            # write_records = int(json.dumps(vertex['metrics']['write-records'], indent=4))

        # duration = json.dumps(y.json()['vertices'][0]['duration'], indent=4)
        # print('duration : ' + duration)
        # metrics = json.dumps(y.json()['vertices'], indent=4)
        # records = json.dumps(y.json()['vertices'][0]['metrics']['write-records'], indent=4)
        # print('records : ' + str(records))

        # executionCostList.append(duration)
        # numberRecordList.append(records)

        z = terminateJob(base_url, job_id)
        print(str(z.status_code) + ", " + z.text)

        # wait at-least 10 seconds before starting next job
        time.sleep(waitBetweenExecutionsSec)

    file = openFile(outputFilePathAndName)

    vertexStr = ""
    for i in range(len(vertexNameList)):
        vertexStr = vertexStr + ", " + vertexNameList[i] + ", " + execDurationList[i] + ", " + readRecordsList[
            i] + ", " + writeRecordsList[i]

    file.write(
        queryOption + "," + approximateQuery + "," + inputTopicName + "," + queryTopicName + "," + outputTopicName + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + queryStreamRate + "," + uniformGridSize + vertexStr + "\n")
    print(
        queryOption + "," + approximateQuery + "," + inputTopicName + "," + queryTopicName + "," + outputTopicName + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + queryStreamRate + "," + uniformGridSize + vertexStr)

    file.flush()
    file.close()


def executeAndSaveJoin(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                       queryStreamRate, uniformGridSize, dateFormat, queryDateFormat,
                       gridMinX, gridMaxX, gridMinY, gridMaxY, qGridMinX, qGridMaxX, qGridMinY, qGridMaxY, trajIDSet,
                       queryPoint, queryPolygon, queryLineString,
                       experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, base_url, jar_id,
                       outputFilePathAndName):
    executionCostList = []
    numberRecordList = []
    vertexNameList = []
    execDurationList = []
    readRecordsList = []
    writeRecordsList = []

    executionCostList.clear()
    numberRecordList.clear()

    for i in range(experimentFrequency):
        parameters = {"programArgsList": ["--onCluster", "true",
                                          "--approximateQuery", approximateQuery,
                                          "--queryOption", queryOption,
                                          "--inputTopicName", inputTopicName,
                                          "--queryTopicName", queryTopicName,
                                          "--outputTopicName", "QueryLatency",
                                          "--inputFormat", "GeoJSON",
                                          "--dateFormat", dateFormat,
                                          "--queryDateFormat", queryDateFormat,
                                          "--radius", radius,
                                          "--aggregate", "SUM",
                                          "--wType", "TIME",
                                          "--wInterval", wInterval,
                                          "--wStep", wStep,
                                          "--uniformGridSize", uniformGridSize,
                                          "--k", k,
                                          "--trajDeletionThreshold", 1000,
                                          "--outOfOrderAllowedLateness", "1",
                                          "--omegaJoinDuration", "1",
                                          "--gridMinX", gridMinX,
                                          "--gridMaxX", gridMaxX,
                                          "--gridMinY", gridMinY,
                                          "--gridMaxY", gridMaxY,
                                          "--qGridMinX", qGridMinX,
                                          "--qGridMaxX", qGridMaxX,
                                          "--qGridMinY", qGridMinY,
                                          "--qGridMaxY", qGridMaxY,
                                          "--trajIDSet", trajIDSet,
                                          "--queryPoint", queryPoint,
                                          "--queryPolygon", queryPolygon,
                                          "--queryLineString", queryLineString],
                      "parallelism": 30}

        x = submitJob(base_url, jar_id, parameters)
        if x.status_code == 200:
            print("Job submitted: " +
                  queryOption + "," + approximateQuery + "," + inputTopicName + "," + queryTopicName + "," + queryDateFormat + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + uniformGridSize)
        else:
            print("Job could not be submitted: " + x.text)

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        print(str(y.status_code) + ", " + y.text)

        while str(json.dumps(y.json()['vertices'][0]['metrics']['write-records-complete'], indent=4)) != "true":
            time.sleep(1)
            y = getJobOverview(base_url, job_id)
            print(str(y.status_code) + ", " + y.text)

        jsonTxt = json.loads(y.text)

        duration = 0
        records = -999999
        for vertex in jsonTxt["vertices"]:
            vertexNameList.append(json.dumps(vertex['name'], indent=4))
            execDurationList.append(json.dumps(vertex['duration'], indent=4))
            readRecordsList.append(json.dumps(vertex['metrics']['read-records'], indent=4))
            writeRecordsList.append(json.dumps(vertex['metrics']['write-records'], indent=4))
            # print(json.dumps(vertex['duration'], indent=4))
            # print(json.dumps(vertex['metrics']['write-records'], indent=4))
            # name = json.dumps(vertex['name'], indent=4)
            # duration = json.dumps(vertex['duration'], indent=4)
            # read_records = int(json.dumps(vertex['metrics']['read-records'], indent=4))
            # write_records = int(json.dumps(vertex['metrics']['write-records'], indent=4))

        # duration = json.dumps(y.json()['vertices'][0]['duration'], indent=4)
        # print('duration : ' + duration)
        # metrics = json.dumps(y.json()['vertices'], indent=4)
        # records = json.dumps(y.json()['vertices'][0]['metrics']['write-records'], indent=4)
        # print('records : ' + str(records))

        # executionCostList.append(duration)
        # numberRecordList.append(records)

        z = terminateJob(base_url, job_id)
        print(str(z.status_code) + ", " + z.text)

        # wait at-least 10 seconds before starting next job
        time.sleep(waitBetweenExecutionsSec)

    file = openFile(outputFilePathAndName)

    # avg_time_ms = average(executionCostList)
    # avg_time_sec = avg_time_ms / 1000
    # avg_records = average(numberRecordList)
    # throughput = avg_records / avg_time_sec

    vertexStr = ""
    for i in range(len(vertexNameList)):
        vertexStr = vertexStr + ", " + vertexNameList[i] + ", " + execDurationList[i] + ", " + readRecordsList[
            i] + ", " + writeRecordsList[i]

    file.write(
        queryOption + "," + approximateQuery + "," + inputTopicName + "," + queryTopicName + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + queryStreamRate + "," + uniformGridSize + vertexStr + "\n")
    print(
        queryOption + "," + approximateQuery + "," + inputTopicName + "," + queryTopicName + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + queryStreamRate + "," + uniformGridSize + vertexStr)

    file.flush()
    file.close()

    # x = getAllJobsOverview(base_url)
    # x = getAllJars(base_url)
    # print(x.status_code)
    # print(x.text)

    # y = json.loads(x.text)
    # for rows in y["jobs"]:
    #    if rows["state"] == "RUNNING":
    #        terminateJob(base_url, rows["jid"])


def executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                   uniformGridSize, dateFormat,
                   gridMinX, gridMaxX, gridMinY, gridMaxY, trajIDSet, queryPoint, queryPolygon, queryLineString,
                   experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, base_url, jar_id,
                   outputFilePathAndName):
    executionCostList = []
    numberRecordList = []

    executionCostList.clear()
    numberRecordList.clear()

    for i in range(experimentFrequency):
        parameters = {"programArgsList": ["--onCluster", "true",
                                          "--approximateQuery", approximateQuery,
                                          "--queryOption", queryOption,
                                          "--inputTopicName", inputTopicName,
                                          "--queryTopicName", "queryTopicName",
                                          "--outputTopicName", "QueryLatency",
                                          "--inputFormat", "GeoJSON",
                                          "--dateFormat", dateFormat,
                                          "--radius", radius,
                                          "--aggregate", "SUM",
                                          "--wType", "TIME",
                                          "--wInterval", wInterval,
                                          "--wStep", wStep,
                                          "--uniformGridSize", uniformGridSize,
                                          "--k", k,
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
                  queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + uniformGridSize)

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        print(str(y.status_code) + ", " + y.text)

        while str(json.dumps(y.json()['vertices'][0]['metrics']['write-records-complete'], indent=4)) != "true":
            time.sleep(1)
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
        time.sleep(waitBetweenExecutionsSec)

    file = openFile(outputFilePathAndName)

    avg_time_ms = average(executionCostList)
    avg_time_sec = avg_time_ms / 1000
    avg_records = average(numberRecordList)
    throughput = avg_records / avg_time_sec

    file.write(
        queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + uniformGridSize + "," + str(
            executionCostList)[1:-1].replace("'", '') + "," + str(avg_time_sec) + ", " + str(numberRecordList)[
                                                                                         1:-1].replace("'",
                                                                                                       '') + "," + str(
            avg_records) + ", " + str(throughput) + "\n")
    print(
        queryOption + "," + approximateQuery + "," + inputTopicName + "," + radius + "," + k + "," + wInterval + "," + wStep + "," + uniformGridSize + "," + str(
            executionCostList)[1:-1].replace("'", '') + "," + str(
            avg_time_sec) + ", " + str(numberRecordList)[
                                   1:-1].replace("'", '') + "," + str(
            avg_records) + ", " + str(throughput))

    file.flush()
    file.close()


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


def calculate(data):
    avg = average(data)
    std = std_deviation(data)
    return (avg, std)


if __name__ == "__main__":
    main()
