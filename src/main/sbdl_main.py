import sys

from lib.logger import Log4j
import lib.utils as utils


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)
    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = utils.get_spark_session(env=job_run_env)
    log = Log4j(spark)
    log.info("Started Spark Application...")

    log.info("Finished creating Spark Session")
