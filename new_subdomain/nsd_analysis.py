import pyspark.sql.functions as F

from utils import log
from config import NSD_PATH


def load_nsd(input_path):
    """Loads NSD Delta Table from the specified path.
    Required columns: domain_name, subdomain

    :param input_path: Path to Delta table
    :return: A DataFrame containing NSD data
    """
    return (
        spark.read
        .format('delta')
        .load(input_path)
        .select('date', 'fqdn', 'domain_name')
    )


def get_top_subdomain_count(top_n=10):
    """Returns domains with the highest subdomain count.

    :param top_n: number of top domains to return
    :return: DataFrame with columns ('domain_name', 'subdomain_count', 'sample_subdomain')
    """
    log('Loading NSD dataset...')
    nsd_df = load_nsd(NSD_PATH)

    log('Aggregating by domain_name...')
    core_domain_count_df = (
        nsd_df
        .groupBy('domain_name')
        .agg(
            F.count('fqdn').alias('subdomain_count'),
            F.first('fqdn').alias('sample_subdomain')
        )
        .orderBy(F.col('subdomain_count').desc())
        .limit(top_n)
    )

    return core_domain_count_df
