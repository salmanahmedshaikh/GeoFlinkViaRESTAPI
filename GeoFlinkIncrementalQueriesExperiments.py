from FlinkRESTAPIMethods import *


def main():
    base_url = "http://localhost:8081/"
    x = getAllJars(base_url)
    # x = getWebUIConfig(base_url)
    # x = uploadJar(base_url, path)
    # x = deleteJar(base_url, jar_id)

    jar_id = "2a47dd7c-3ebd-4a4e-ac98-7088567e55bb_GeoFlinkProject-0.2.jar"

    experimentFrequency = 2
    executionTimeSeconds = 120
    waitBetweenExecutionsSec = 60

    outputTopicName = "SpatialTrajs1000IDs20Million"
    bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092"
    dataRows = "100000"
    # dataObjIDRange = "1000"
    queryRandomOption = "gaussianRW"
    queryDatatypeOption = "point"
    queryParallelism = 30

    parameters = {"programArgsList": ["-DdataGen.clusterMode=true",
                                      "-Dgeoflink.output.kafka.outputTopicName=" + outputTopicName,
                                      "-Dgeoflink.output.kafka.bootStrapServers=" + bootStrapServers,
                                      "-Dgeoflink.output.data.nRows=" + dataRows,
                                      "-Dgeoflink.query.randomOption=" + queryRandomOption,
                                      "-Dgeoflink.query.datatypeOption=" + queryDatatypeOption
                                      ]}

    x = submitJob(base_url, jar_id, parameters)

    if x.status_code == 200:
        print(str(datetime.now()) + " Job submitted")
    else:
        print(str(datetime.now()) + " Job could not be submitted: " + x.text)







if __name__ == "__main__":
    main()
