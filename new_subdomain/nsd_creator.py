import pyspark
import pyspark.sql.functions as F
from datetime import datetime, timedelta

from new_subdomain.config import NCD_PATH, NSD_PATH
from new_subdomain.config import MAX_SUBDOMAIN_DEPTH, WINDOW_DAYS, DOMAIN_WHITELIST
from new_subdomain.utils import log


def compute_new_subdomains(
    input_path: str = NCD_PATH,
    output_path: str = NSD_PATH,
    nsd_date: str = '2023-06-13',
    window_days: int = WINDOW_DAYS,
    subdomain_depth: int = MAX_SUBDOMAIN_DEPTH,
    domain_whitelist: list = DOMAIN_WHITELIST
):
    """The main method that creates a New SubDomain (NSD) dataset from New Core Domain (NCD) dataset.

    :param input_path: Path to NCD dataset (str)
    :param output_path: Path to output NSD dataset (str)
    :param nsd_date: Date for which subdomains are calculated (str: '%Y-%m-%d')
    :param window_days: Size of window in days (int)
    :param subdomain_depth: Maximum subdomain level (int)
    :param domain_whitelist: Domains that should be ignored (list[str])
    :return: A new DataFrame, whose contents have been written to output_path
    """
    nsd_date = datetime.strptime(nsd_date, '%Y-%m-%d').date()
    window_start = nsd_date - timedelta(days=window_days)

    log("Loading NCD dataset...")
    ncd_df = load_ncd(input_path)

    log("Filtering NCD...")
    filtered_ncd_df = filter_ncd(ncd_df, window_start)
    filtered_ncd_df = filter_whitelist_ncd(filtered_ncd_df, whitelist=domain_whitelist)

    log("Extracting subdomains...")
    subdomains_df = extract_subdomains(filtered_ncd_df, subdomain_depth=subdomain_depth)
    subdomains_df = subdomains_df.dropDuplicates()

    log("Aggregating subdomains...")
    new_subdomains_df = aggregate_subdomains(subdomains_df, nsd_date)

    log(f"Saving results to {output_path}")
    save_results(new_subdomains_df, output_path)

    return new_subdomains_df


def load_ncd(input_path):
    """Reads NCD dataset from the specified path.

    :param input_path: Path to Delta table with NCD data
    :return: DataFrame with columns ('date', 'fqdn', 'domain_name')
    """
    return (
        pyspark.read
        .format('delta')
        .load(input_path)
        .select('date', 'fqdn', 'domain_name')
    )


def filter_ncd(ncd_df, window_start):
    """Applies filtering for the NCD DataFrame.

    :param ncd_df: Base DataFrame to be filtered
    :param window_start: The earliest date accepted by the filter
    :return: Filtered DataFrame
    """
    return (
        ncd_df
        .filter(F.col('date') >= window_start)
        .filter(F.col('domain_name').isNotNull())
        .filter(F.col('domain_name') != '')
    )


def filter_whitelist_ncd(ncd_df, whitelist=DOMAIN_WHITELIST):
    """Filters NCD by the provided core domain whitelist.

    :param ncd_df: Base DataFrame to be filtered
    :param whitelist: List of core domains to filter by
    :return: Filtered DataFrame
    """
    return (
        ncd_df
        .filter(~F.col('domain_name').isin(*whitelist))
    )


def extract_subdomains(ncd_df, subdomain_depth=MAX_SUBDOMAIN_DEPTH):
    """Extracts subdomains of expected length from NCD FQDN

    :param ncd_df: Base DataFrame, from which subdomains will be extracted
    :param subdomain_depth: numbers of labels accepted *before* core domain
        i.e. core domain = "a.b.c", fqdn = "sub2.sub1.a.b.c", with depth 1
            will return "sub1.a.b.c"
    :return: Base DataFrame, augmented with "subdomain" column
    """
    return (
        ncd_df
        .withColumn('labels', F.split('fqdn', r'\.'))
        .withColumn('domain_labels', F.split('domain_name', r'\.'))
        .withColumn(
            'subdomain_label_start',
            F.size('labels') - F.size('domain_labels') - subdomain_depth + 1
        )
        .withColumn(
            'subdomain_labels',
            F.slice(
                F.col('labels'),
                start=F.greatest(F.lit(1), F.col('subdomain_label_start')),
                length=F.size('labels')
            )
        )
        .withColumn('subdomain', F.concat_ws('.', F.col('subdomain_labels')))
        .select("date", "domain_name", "subdomain")
    )


def aggregate_subdomains(subdomains_df, nsd_date):
    """The actual NCD blackbox. This function removes all subdomains who appeared in traffic before nsd_date.

    :param subdomains_df: Dataframe with extracted subdomains
    :param nsd_date: date for calculating NSD
    :return: New SubDomain Dataset for given nsd_date with columns ('subdomain', 'domain_name', 'first_seen')
    """
    return (
        subdomains_df
        .groupBy('subdomain')
        .agg(
            F.first('domain_name').alias('domain_name'),
            F.min('date').alias('first_seen')
        )
        .filter(F.col('date') == nsd_date)
    )


def save_results(results_df, output_path):
    """Saves provided DataFrame under output_path

    :param results_df: A DataFrame
    :param output_path: Path to the output Delta Table
    """
    (
        results_df
        .write
        .mode("append")
        .format("delta")
        .partitionBy('nsd_date')
        .option("mergeSchema", "true")
        .save(output_path)
    )
