from new_subdomain.nsd_creator import compute_new_subdomains
from new_subdomain.nsd_analysis import get_top_subdomain_count

from new_subdomain.config import NCD_PATH, NSD_PATH
from new_subdomain.config import WINDOW_DAYS, MAX_SUBDOMAIN_DEPTH, DOMAIN_WHITELIST

nsd_df = compute_new_subdomains(
    input_path=NCD_PATH,
    output_path=NSD_PATH,
    nsd_date='2023-06-14',
    window_days=WINDOW_DAYS,
    subdomain_depth=MAX_SUBDOMAIN_DEPTH,
    domain_whitelist=DOMAIN_WHITELIST
)

nsd_df.show()

#    date    |       core domain        | Subdomain
# 2023-06-14 | -vno.blogspot.tw         | -vno.blogspot.tw
# 2023-06-14 | mcafee.com               | avqs.mcafee.com
# 2023-06-14 | semver.org               | 008motoren.semver.org
# 2023-06-14 | layerswp.com             | 00myangtsel4.layerswp.com
# 2023-06-14 | 03d0.webhop.info         | 03d0.webhop.info
# 2023-06-14 | walmartmobile.cn         | 041d64f464cde923b09e29d6.walmartmobile.cn
# 2023-06-14 | 08aq.from-va.com         | 08aq.from-va.com
# 2023-06-14 | your-server.de           | dedi1208.your-server.de
# 2023-06-14 | 09ru.homeftp.net         | 09ru.homeftp.net
# 2023-06-14 | 0jif.applicationcloud.io | 0jif.applicationcloud.io

top_subdomain_count = get_top_subdomain_count(top_n=10)
top_subdomain_count.show()

# domain_name       |  subdomain_count  | sample_subdomain
# anymeeting.com	|3979743        	| 01fs5.anymeeting.com
# myonlinedata.net	|3767057        	| 02-mad.myonlinedata.net
# ampproject.net	|2441390        	| d-10001605022397645554.ampproject.net
# blackrock.com	    |2407769        	| 0-hh.blackrock.com
# casaemcasa.com.br	|2299775        	| 000420-bst-c0178c3a-5f7b-4453-adac-3499bb812dbb-1.casaemcasa.com.br
# basemarket.com.br	|2200894        	| 00107fd04be10-e566b4338d32ec896d89fa9752c681fd.basemarket.com.br
# bfm.com	        |2194912        	| 0-duckietv.bfm.com
# artnova.com.br	|1929314        	| 011e960bb703bst-dd6cfc0b-3117-401d-a250-618bc322312d-1.artnova.com.br
# cachematrix.com	|1891714        	| 02caldav.cachematrix.com
# akamaihd.net	    |1872269        	| 1-36-226-154_s-219-76-10-26_ts-1686605001-clienttons-s.akamaihd.net
