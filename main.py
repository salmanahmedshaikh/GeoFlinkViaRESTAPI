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

    jar_id = "79da2b57-386f-409d-9964-4e55c0aa221c_GeoFlinkProject20210315.jar"
    experimentFrequency = 3
    executionTimeSeconds = 120
    waitBetweenExecutionsSec = 10

    # Range Query
    ''' 
    outputFilePathAndName = "qureyOutput_RangeQuery.csv"
    inputTopicNameList = ["TaxiDrive17MillionGeoJSON", "NYCBuildingsPolygons", "NYCBuildingsLineStrings"]
    ifApproximateQuery = ["true", "false"]
    radiusList = ["0.0005", "0.005", "0.05", "0.5"]
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
    trajIDSet = ""
    queryPoint = ""
    queryPolygon = ""
    queryLineString = ""

    file = openFile(outputFilePathAndName)
    file.write(
        "queryOption" + "," + "approximateQuery" + "," + "inputTopicName" + "," + "radius" + "," + "wInterval" + "," + "wStep" + "," + "uniformGridSize" + "," + "executionCost1, executionCost2, executionCost3" + "," + "avg_time_sec" + ", " + "numberRecords1, numberRecords2, numberRecords3, " + "avg_records" + ", " + "throughput" + "\n")
    file.flush()
    file.close()

    for inputTopicName in inputTopicNameList:
        if inputTopicName == "TaxiDrive17MillionGeoJSON":
            queryOptionListWindowed = ["1", "6", "11"]
            queryOptionListRealtime = ["2", "7", "12"]
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
            queryOptionListWindowed = ["16", "21", "26"]
            queryOptionListRealtime = ["17", "22", "27"]
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
            queryOptionListWindowed = ["31", "36", "41"]
            queryOptionListRealtime = ["32", "37", "42"]
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
            for approximateQuery in ifApproximateQuery:
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for wInterval in wIntervalList:
                approximateQuery = "true"
                radius = "0.005"
                wStep = "50"
                uniformGridSize = "200"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for wStep in wStepList:
                approximateQuery = "true"
                radius = "0.005"
                uniformGridSize = "200"
                wInterval = "100"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

        for queryOption in queryOptionListRealtime:
            for approximateQuery in ifApproximateQuery:
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)
                               
   '''

    '''
    # kNN Query
    outputFilePathAndName = "qureyOutput_kNNQuery.csv"
    inputTopicNameList = ["TaxiDrive17MillionGeoJSON", "NYCBuildingsPolygons", "NYCBuildingsLineStrings"]
    kList = ["25", "50", "75", "100", "125"]
    radiusList = ["0.0005", "0.005", "0.05", "0.5"]
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
    trajIDSet = ""
    queryPoint = ""
    queryPolygon = ""
    queryLineString = ""

    file = openFile(outputFilePathAndName)
    file.write(
        "queryOption" + "," + "approximateQuery" + "," + "inputTopicName" + "," + "radius" + "," + "k" + "," + "wInterval" + "," + "wStep" + "," + "uniformGridSize" + "," + "executionCost1, executionCost2, executionCost3" + "," + "avg_time_sec" + ", " + "numberRecords1, numberRecords2, numberRecords3, " + "avg_records" + ", " + "throughput" + "\n")
    file.flush()
    file.close()

    for inputTopicName in inputTopicNameList:
        if inputTopicName == "TaxiDrive17MillionGeoJSON":
            queryOptionListWindowed = ["51", "56", "61"]
            queryOptionListRealtime = ["52", "57", "62"]
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
            queryOptionListWindowed = ["66", "71", "76"]
            queryOptionListRealtime = ["67", "72", "77"]
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
            queryOptionListWindowed = ["81", "86", "91"]
            queryOptionListRealtime = ["82", "87", "92"]
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
            for k in kList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for wInterval in wIntervalList:
                approximateQuery = "true"
                radius = "0.005"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for wStep in wStepList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

        for queryOption in queryOptionListRealtime:
            for k in kList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)
   '''

    # Join Query
    outputFilePathAndName = "qureyOutput_JoinQuery.csv"
    inputTopicNameList = ["TaxiDrive17MillionGeoJSON", "NYCBuildingsPolygons", "NYCBuildingsLineStrings"]
    radiusList = ["0.0005", "0.005", "0.05", "0.5"]
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
    trajIDSet = ""
    queryPoint = ""
    queryPolygon = ""
    queryLineString = ""

    file = openFile(outputFilePathAndName)
    file.write(
        "queryOption" + "," + "approximateQuery" + "," + "inputTopicName" + "," + "radius" + "," + "k" + "," + "wInterval" + "," + "wStep" + "," + "uniformGridSize" + "," + "executionCost1, executionCost2, executionCost3" + "," + "avg_time_sec" + ", " + "numberRecords1, numberRecords2, numberRecords3, " + "avg_records" + ", " + "throughput" + "\n")
    file.flush()
    file.close()

    for inputTopicName in inputTopicNameList:
        if inputTopicName == "TaxiDrive17MillionGeoJSON":
            queryOptionListWindowed = ["101", "106", "111"]
            queryOptionListRealtime = ["102", "107", "112"]
            queryTopicName = "TaxiDriveQuery"
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
            queryTopicName = "NYCBuildingsPolygonsQuery"
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
            queryTopicName = "NYCBuildingsLineStringsQuery"
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

            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for wInterval in wIntervalList:
                approximateQuery = "true"
                radius = "0.005"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                               wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for wStep in wStepList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                               wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                               wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

        for queryOption in queryOptionListRealtime:
            for radius in radiusList:
                approximateQuery = "true"
                wInterval = "100"
                wStep = "50"
                uniformGridSize = "200"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                               wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

            for uniformGridSize in uniformGridSizeList:
                approximateQuery = "true"
                radius = "0.005"
                wInterval = "100"
                wStep = "50"
                k = "50"
                executeAndSave(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval,
                               wStep,
                               uniformGridSize, dateFormat, gridMinX, gridMaxX, gridMinY, gridMaxY,
                               trajIDSet, queryPoint, queryPolygon, queryLineString,
                               experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec,
                               base_url, jar_id, outputFilePathAndName)

    # x = getAllJobsOverview(base_url)
    # x = getAllJars(base_url)
    # print(x.status_code)
    # print(x.text)

    # y = json.loads(x.text)
    # for rows in y["jobs"]:
    #    if rows["state"] == "RUNNING":
    #        terminateJob(base_url, rows["jid"])


def executeAndSave(queryOption, approximateQuery, inputTopicName, queryTopicName, radius, k, wInterval, wStep, uniformGridSize, dateFormat,
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
                                          "--queryTopicName", queryTopicName,
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
