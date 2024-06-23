import time
from pyspark.sql import SparkSession
import shutil
import os


class FileStream:
    def __init__(self):
        self.spark = SparkSession.builder.appName("myApp").master("local[2]").getOrCreate()
        self.schema = "OrderNumber int,OrderDate string,ItemName string,Quantity int,ProductPrice float,TotalProducts int"

    def createStream(self):
        # Start a streaming query to read the CSV file.
        stream = self.spark.readStream.schema(self.schema).format("csv"). \
            load('/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/source_data/*')
        return stream

    def writeStreamToConsole(self, stream):
        stream_query = stream.writeStream.option("checkpointLocation",
                                                 "/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo"
                                                 "/checkpoint/checkdir1"). \
            format("console").start()
        return stream_query

    @classmethod
    def load_data_to_source(cls):
        # copy the contents of the datasets to source folder
        itr = 1
        while itr < 6:
            shutil.copyfile(f'/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/datasets/data{itr}.csv',
                            f'/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/source_data/data{itr}.csv')
            time.sleep(8)
            itr += 1

    @classmethod
    def delete_files_in_directory(cls, directory_path):
        try:
            files = os.listdir(directory_path)
            for file in files:
                file_path = os.path.join(directory_path, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            print("All files deleted from folder successfully.")
        except OSError:
            print("Error occurred while deleting files.")


if __name__ == '__main__':
    dir_path1 = '/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/source_data/'
    dir_path2 = '/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/checkpoint/checkdir1/'
    fs = FileStream()
    # clear the sample files in the source folder and checkpoint dir
    fs.delete_files_in_directory(dir_path1)
    shutil.rmtree(dir_path2)
    stream = fs.createStream()
    stream_query = fs.writeStreamToConsole(stream)
    fs.load_data_to_source()
    # stream_query.awaitTermination()
    stream_query.stop()
