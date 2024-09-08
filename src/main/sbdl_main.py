import os
import sys

from lib.logger import Log4j
import lib.utils as utils

import businesslogic as bo


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)
    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = utils.get_spark_session(env=job_run_env)
    log = Log4j(spark)
    log.info("Started Spark Application...")

    accounts_file_path = os.path.join(
        "src\\resources\\data\\accounts\\account_samples.csv"
    )
    party_file_path = os.path.join("src\\resources\\data\\parties\\party_samples.csv")
    address_file_path = os.path.join(
        "src\\resources\\data\\party_address\\address_samples.csv"
    )

    accounts_df = bo.read_csv_data(spark, accounts_file_path)
    party_df = bo.read_csv_data(spark, party_file_path)
    address_df = bo.read_csv_data(spark, address_file_path)

    log.info("Finished creating Spark Session")
