class DatabaseReader:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def create_dataframe(self,spark,table_name):
        # df = spark.read.jdbc(url=self.url,
        #                      table=table_name,
        #                      properties=self.properties)
        df = spark.read.jdbc(url="jdbc:mysql://localhost:3306/youtube_project",
                             table=table_name,
                             properties=self.properties)

        return df