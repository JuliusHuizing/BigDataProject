    @staticmethod
    def augment_with_vine_columns(data: DataSet) -> DataSet:
        # Define UDF to convert 'Y' values to 1 and 'N' values to 0 for 'vine' column
        binary_udf = udf(lambda x: 1 if x == 'Y' else 0, IntegerType())

        # Add columns for vine_Y and vine_N
        data.df = data.df.withColumn("vine_Y", binary_udf(col("vine")))
        data.df = data.df.withColumn("vine_N", 1 - col("vine_Y"))  # vine_N is the complement of vine_Y

        return data

    @staticmethod
    def augment_with_verified_columns(data: DataSet) -> DataSet:
        # Define UDF to convert 'Y' values to 1 and 'N' values to 0 for 'verified_purchase' column
        binary_udf = udf(lambda x: 1 if x == 'Y' else 0, IntegerType())

        # Add columns for verified_Y and verified_N
        data.df = data.df.withColumn("verified_Y", binary_udf(col("verified_purchase")))
        data.df = data.df.withColumn("verified_N", 1 - col("verified_Y"))  # verified_N is the complement of verified_Y

        return data