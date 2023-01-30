import os

directoryPath = "/data1/datasets/2D_Spatial/T-driveTaxiTrajectories/taxi_log_2008_by_id_GeoJSON/"
directoryPath = "/data1/datasets/2D_Spatial/T-driveTaxiTrajectories/taxi_log_2008_by_id_GeoJSON/"

filesArray = os.listdir(directoryPath)

files = [open(directoryPath + fileName, "r") for fileName in filesArray]

# Counting average traj length
sumOfLines = 0
for i in range(len(filesArray)):
    file = open(directoryPath + filesArray[i], "r")
    lineCount = len(file.readlines())
    sumOfLines += lineCount

print("sumOfLines ", sumOfLines)
print("num of files ", (len(filesArray)))
print("Average traj length ", sumOfLines/(len(filesArray)))