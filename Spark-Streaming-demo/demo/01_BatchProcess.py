from pyspark.sql import SparkSession


class BatchProcess:
    def __init__(self):
        self.spark = SparkSession.builder.appName("myApp").master("local[2]").getOrCreate()
        self.schema = "OrderNumber int,OrderDate string,ItemName string,Quantity int,ProductPrice float,TotalProducts int"
        self.file_path = '/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/output/'

    def loadData(self):
        # Start to read the CSV file.
        df = self.spark.read.schema(self.schema).format("csv"). \
            load('/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/source_data/*')
        return df

    def writeData(self, dataframe):
        dataframe.write.format("csv").mode("overwrite").save(self.file_path)


    # @classmethod
    # def load_data_to_source(cls):
    #     # copy the contents of the datasets to source folder
    #     itr = 1
    #     while itr < 6:
    #         shutil.copyfile(f'/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/datasets/data{itr}.csv',
    #                         f'/Users/paresh.jadhav/PycharmProjects/Spark-Streaming-demo/data/source_data/data{itr}.csv')
    #         time.sleep(8)
    #         itr += 1
    #
    # @classmethod
    # def delete_files_in_directory(cls, directory_path):
    #     try:
    #         files = os.listdir(directory_path)
    #         for file in files:
    #             file_path = os.path.join(directory_path, file)
    #             if os.path.isfile(file_path):
    #                 os.remove(file_path)
    #         print("All files deleted from folder successfully.")
    #     except OSError:
    #         print("Error occurred while deleting files.")


if __name__ == '__main__':
    batch = BatchProcess()
    df = batch.loadData()
    batch.writeData(df)

