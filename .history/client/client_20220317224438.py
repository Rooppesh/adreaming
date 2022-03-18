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
"""The Python implementation of the GRPC helloworld.Greeter client."""

from __future__ import print_function
from ctypes import sizeof

import time
import logging
from flask import before_render_template
import pandas
import grpc
import helloworld_pb2
import helloworld_pb2_grpc
import findBandwidth from ../server/
def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    row_count = 0
    itr = 1
    fileReader = open("../data/received_data.csv", "r")
    fileAppender = open("../data/received_data.csv", "a")
    yAxisWriter = open("../data/y_axis.csv", "a")

    with grpc.insecure_channel('169.254.211.61:50051') as channel:
        stub = helloworld_pb2_grpc.GetterStub(channel)
        currentBandwidth = findBandwidth()

        if currentBandwidth >= 80 : 
            while row_count < 4194304: 
                row_count += int((sum(1 for row in fileReader))/8) 
                # row_count = int(row_count/8)
                print("wrote row " + str(row_count))
                # row_count = int((row_count - (row_count%8))/8)
                beforeTime = time.time() 
                response = stub.GetIMDBData(helloworld_pb2.Request(rowOffset=row_count))
                afterTime = time.time()
                fileAppender.write(str(response.results))
                # print("Time elapsed for write " + str(itr) + ": " + str(afterTime-beforeTime))
                yAxisWriter.write(str(afterTime-beforeTime)+"\n")
                itr = itr+1
        elif currentBandwidth >= 64:
            while row_count < 4194304: 
                row_count += int((sum(1 for row in fileReader))/8) 
                # row_count = int(row_count/8)
                print("wrote row " + str(row_count))
                # row_count = int((row_count - (row_count%8))/8)
                beforeTime = time.time() 
                response = stub.GetIMDBData(helloworld_pb2.Request(rowOffset=row_count))
                afterTime = time.time()
                fileAppender.write(str(response.results))
                # print("Time elapsed for write " + str(itr) + ": " + str(afterTime-beforeTime))
                yAxisWriter.write(str(afterTime-beforeTime)+"\n")
                itr = itr+1
        else: 
            while row_count < 4194304: 
                row_count += int((sum(1 for row in fileReader))/8) 
                # row_count = int(row_count/8)
                print("wrote row " + str(row_count))
                # row_count = int((row_count - (row_count%8))/8)
                beforeTime = time.time() 
                response = stub.GetIMDBData(helloworld_pb2.Request(rowOffset=row_count))
                afterTime = time.time()
                fileAppender.write(str(response.results))
                # print("Time elapsed for write " + str(itr) + ": " + str(afterTime-beforeTime))
                yAxisWriter.write(str(afterTime-beforeTime)+"\n")
                itr = itr+1
    fileReader.close()
    fileAppender.close()


if __name__ == '__main__':
    logging.basicConfig()
    run()
