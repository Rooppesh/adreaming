# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC helloworld.Greeter server."""

from concurrent import futures
import logging
from sqlite3 import Row
import pandas
import grpc
import helloworld_pb2
import helloworld_pb2_grpc
import time
import psutil

class Getter(helloworld_pb2_grpc.GetterServicer):

    def GetIMDBData(self, request, context):
        imdbData = pandas.read_csv("../data/title_basics.csv", header=1, skiprows=request.rowOffset)

        # imdbDataSize = find length of DF
        # currentBandwidth = add logic to find current bandwidth
        # numberOfPackets = add logic to find number of packets to split the dataset into depending on currentBandwidth 
        # numberOfRowsPerPacket = len(imdbData)/numberOfPackets

        print(psutil.cpu_percent(5))

        # value = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
        # bandwidth = value/1024./1024.*8
        # print(bandwidth)
        
        resultObject = helloworld_pb2.Result()

        for i in range(request.rowOffset, request.rowOffset+5000):
            dataPacket = imdbData.iloc[i]
            rowobject = resultObject.results.add()
            rowobject.tconst=str(dataPacket[0])
            rowobject.titleType=str(dataPacket[1])
            rowobject.primaryTitle=str(dataPacket[2])
            rowobject.originalTitle=str(dataPacket[3])
            rowobject.isAdult=dataPacket[4]
            rowobject.startYear=str(dataPacket[5])
            rowobject.endYear=str(dataPacket[6])
            rowobject.runtimeMinutes=str(dataPacket[7])
            rowobject.genres=str(dataPacket[8])
            
        print(psutil.cpu_percent(5))
        return resultObject


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    helloworld_pb2_grpc.add_GetterServicer_to_server(Getter(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    serve()
