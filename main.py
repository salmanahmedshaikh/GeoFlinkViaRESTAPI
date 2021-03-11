import requests
import os
import json
import time


def main():
    base_url = "http://localhost:29999/"

    x = getAllJars(base_url)
    # x = getWebUIConfig(base_url)
    # path = "/home/salman/Development/Flink/Projects/GeoFlink/target/GeoFlinkProject-0.1.jar"
    # path = "/home/salman/Development/Flink/Projects/GeoFlinkExperiments/target/GeoFlinkProject-0.1.jar"
    # path = "/data1/development/Flink/Projects/MFEventTimeGeoFlink/target/GeoFlinkProject-0.1.jar"
    # x = uploadJar(base_url, path)
    jar_id = "55baf91f-11de-400d-95ba-84852d293c38_GeoFlinkProject20210309_2.jar"

    # test
    # x = deleteJar(base_url, jar_id)


    # --queryOption "stayTime" --inputs "{movingObjTopics: [MovingFeatures, MovingFeatures2], sensorTopics: [MovingFeatures2]}" --output "outputTopic" --queryId "Q1" --sensorRadius "0.0005" --aggregate "AVG" --cellLength "10.0" --wType "TIME" --wInterval "10" --wStep "10" --gType "Polygon" --gCoordinates "{coordinates: [[[139.7766, 35.6190], [139.7766, 35.6196], [139.7773, 35.6190],[139.7773, 35.6196], [139.7766, 35.6196]]]}"
    # parameters = {
    #     "programArgsList": ["--queryOption", "stayTime", "--inputs", "{movingObjTopics: [MovingFeatures, MovingFeatures2], sensorTopics: [MovingFeatures2]}", "--output", "output", "--queryId", "Q1", "--sensorRadius", "0.0005",
    #                         "--aggregate", "AVG", "--cellLength", "10.0", "--wType", "TIME", "--wInterval", "10", "--wStep", "10", "--gType", "Polygon", "--gCoordinates",
    #                         "{coordinates: [[[139.77667562042726, 35.6190837], [139.77667562042726, 35.6195177], [139.77722108273215, 35.6190837],[139.77722108273215, 35.6195177], [139.77667562042726, 35.6190837]]]}",
    #                         "--gPointCoordinates", "{coordinates: [139.77667562042726, 35.6190837]"
    #                         ],
    #     "parallelism": 30
    # }

    # parameters = {"programArgsList": ["--queryOption", "stayTimeAngularGrid", "--inputs",
    #                                   "{movingObjTopics: [TaxiDriveGeoJSON_17M_R2_P60]}", "--output", "outputTopic",
    #                                   "--queryId", "Q1", "--sensorRadius", "0.0005", "--aggregate", "AVG",
    #                                   "--cellLength", "360", "--wType", "TIME", "--wInterval", "10", "--wStep", "10",
    #                                   "--gType", "Polygon", "--gCoordinates",
    #                                   "{coordinates: [[[115.50000, 39.60000], [115.50000, 41.10000], [117.60000, 41.10000],[117.60000, 39.60000], [115.50000, 39.60000]]]}",
    #                                   "--gPointCoordinates", "{coordinates: [115.50000, 39.60000]}", "--gRows", "50",
    #                                   "--gColumns", "20", "--gAngle", "45"], "parallelism": 10}

    # parameters = {"programArgsList": ["--queryOption", "stayTimeAngularGrid", "--inputs", "{movingObjTopics: [GeoJSONwithCRS03]}",
    #                      "--output", "testOutput42", "--queryId", "testQ1", "--GCSCoordinates", "false", "--sensorRadius", "0.0005", "--aggregate",
    #                      "AVG", "--cellLength", "1", "--wType", "TIME", "--wInterval", "10", "--wStep", "10", "--gType",
    #                      "--Polygon", "--gCoordinates",
    #                      "{coordinates: [[[115.50000, 39.60000], [115.50000, 41.10000], [117.60000, 41.10000],[117.60000, 39.60000], [115.50000, 39.60000]]]}",
    #                      "--gPointCoordinates", "{coordinates: [-5061.771566548391,-42385.84023166007]}", "--gRows", "96", "--gColumns", "108",
    #                      "--gAngle", "0"], "parallelism": 10}

    #parameters = {"programArgsList": ["--onCluster", "true",
    #                                  "--queryOption", "27",
    #                                  "--aggregate", "SUM",
    #                                  "--wType", "TIME",
    #                                  "--wInterval", "5",
    #                                  "--wStep", "5",
    #                                  "--uniformGridSize", 100,
    #                                  "--k", "99",
    #                                  "--radius", "1000",
    #                                  "--inputTopicName", "TaxiDrive17MillionGeoJSON",
    #                                  "--dateFormat", "yyyy-MM-dd HH:mm:ss",
    #                                  "--dataset", "TDriveBeijing",
    #                                  "--outputTopicName", "TDriveLatency27",
    #                                  "--inputFormat", "GeoJSON",
    #                                  "--queryTopicName", "ATCQueryStream",
    #                                  "--trajDeletionThreshold", 1000,
    #                                  "--outOfOrderAllowedLateness", "1",
    #                                  "--omegaJoinDuration", "1"],
    #              "parallelism": 30}


    parameters = {"programArgsList": ["--onCluster", "true",
                                      "--approximateQuery", "false",
                                      "--queryOption", "31",
                                      "--inputTopicName", "NYCBuildingsLineStrings",
                                      "--queryTopicName", "sampleTopic",
                                      "--outputTopicName", "QueryLatency",
                                      "--inputFormat", "GeoJSON",
                                      "--dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                                      "--radius", "0.05",
                                      "--aggregate", "SUM",
                                      "--wType", "TIME",
                                      "--wInterval", "50",
                                      "--wStep", "15",
                                      "--uniformGridSize", 100,
                                      "--k", "10",
                                      "--trajDeletionThreshold", 1000,
                                      "--outOfOrderAllowedLateness", "1",
                                      "--omegaJoinDuration", "1",
                                      "--gridMinX", "-74.25540",
                                      "--gridMaxX", "-73.70007",
                                      "--gridMinY", "40.49843",
                                      "--gridMaxY", "40.91506",
                                      "--trajIDSet", "9211800, 9320801, 9090500, 7282400, 10390100",
                                      "--queryPoint", "[-74.0000, 40.72714]",
                                      "--queryPolygon", "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]",
                                      "--queryLineString", "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"],
                  "parallelism": 30}

    #"--gridMinX", "115.50000",
    #"--gridMaxX", "117.60000",
    #"--gridMinY", "39.60000",
    #"--gridMaxY", "41.10000",
    #"--queryPoint", "[116.14319183444924, 40.07271444145411]",
    #"--queryPolygon", "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762], [116.14319183444924, 40.07271444145411]",
    #"--queryLineString", "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762]"],



    # parameters = {"programArgsList": ["--onCluster", "true", "--queryOption", "27", "--inputTopicName",
    #                                   "ATCShoppingMall", "--dataset", "ATCShoppingMall", "--outputTopicName",
    #                                   "ATCLatency27", "--inputFormat",
    #                                   "GeoJSON", "--dateFormat", "null", "--aggregate", "SUM", "--wType",
    #                                   "TIME", "--wInterval", "10", "--wStep", "5",
    #                                    "--uniformGridSize", "100", "--k", "99", "--radius", "2000", "--queryTopicName", "ATCQueryStream", "--trajDeletionThreshold", "1000"], "parallelism": 30}

    #x = getAllJobsOverview(base_url)

    #y = json.loads(x.text)
    #for rows in y["jobs"]:
    #    if rows["state"] == "RUNNING":
    #        terminateJob(base_url, rows["jid"])

    x = submitJob(base_url, jar_id, parameters)

    #x = getFlinkClusterOverview(base_url)＃
    #x = terminateJob(base_url, job_id)

    print(x.status_code)
    print(x.text)

    #job_id = "fb9c48cf5d34527e2f3a437244f6e8de"
    #x = getJobOverview(base_url, job_id)

    job_id = json.dumps(x.json()['jobid'], indent=4)
    y = getJobOverview(base_url, job_id.replace('"', ''))
    print(y.status_code)
    print(y.text)




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


if __name__ == "__main__":
    main()
