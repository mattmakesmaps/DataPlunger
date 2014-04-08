__author__ = 'matt'
__date__ = '3/29/14'

"""
Experiment using timeit to compare speed of dictionary population methods.

dict_comp_iteritems = {key: value for key, value in inLine.iteritems() if key in self.out_fields}
dict_comp_items = {key: value for key, value in inLine.items() if key in self.out_fields}
update_iteritems = inLine.update((k, v) for k, v in inLine.iteritems() if k in self.out_fields)
update_items = inLine.update((k, v) for k, v in inLine.items() if k in self.out_fields)

Results:
test_update_iteritems()
9.41458487511
test_update_items()
10.392802
test_dict_comp_iteritems()
6.66995000839
test_dict_comp_items()
7.45588612556
"""

if __name__ == '__main__':
    import timeit
    setup_statement = """in_line = {'LATITUDE83': '', 'CREATE_DATE': '01-MAR-00', 'FEDERAL_FACILITY_CODE': '', 'PGM_SYS_ACRNMS': 'NPDES', 'FIPS_CODE': '02016', 'REF_POINT_DESC': '', 'HUC_CODE': '', 'LOCATION_DESCRIPTION': '', 'LONGITUDE83': '', 'PRIMARY_NAME': 'ADAK FACILITY', 'CITY_NAME': 'ADAK', 'ACCURACY_VALUE': '', 'UPDATE_DATE': '05-MAR-13', 'STATE_NAME': 'ALASKA', 'CONVEYOR': '', 'INTEREST_TYPES': 'ICIS-NPDES NON-MAJOR', 'FRS_FACILITY_DETAIL_REPORT_URL': 'http://iaspub.epa.gov/enviro/fii_query_detail.disp_program_facility?p_registry_id=110009691342', 'US_MEXICO_BORDER_IND': '', 'COUNTY_NAME': 'ALEUTIANS WEST', 'HDATUM_DESC': 'NAD83', 'LOCATION_ADDRESS': '100 SUPPLY ROAD', 'SITE_TYPE_NAME': 'STATIONARY', 'TRIBAL_LAND_CODE': '', 'SIC_CODES': '2091, 2092', 'FEDERAL_AGENCY_NAME': '', 'SIC_CODE_DESCRIPTIONS': 'CANNED AND CURED FISH AND SEAFOODS, PREPARED FRESH OR FROZEN FISH AND SEAFOODS', 'NAICS_CODES': '', 'CENSUS_BLOCK_CODE': '', 'POSTAL_CODE': '99546-1884', 'COUNTRY_NAME': 'USA', 'TRIBAL_LAND_NAME': '', 'COLLECT_DESC': '', 'SOURCE_DESC': '', 'SUPPLEMENTAL_LOCATION': 'P.O. BOX 1884', 'CONGRESSIONAL_DIST_NUM': '', 'EPA_REGION_CODE': '10', 'STATE_CODE': 'AK', 'REGISTRY_ID': '110009691342', 'NAICS_CODE_DESCRIPTIONS': ''}; out_fields = set(['LATITUDE83', 'CREATE_DATE', 'FEDERAL_FACILITY_CODE', 'PGM_SYS_ACRNMS', 'FIPS_CODE', 'REF_POINT_DESC', 'HUC_CODE', 'LOCATION_DESCRIPTION', 'LONGITUDE83', 'PRIMARY_NAME', 'CITY_NAME', 'ACCURACY_VALUE', 'UPDATE_DATE', 'STATE_NAME', 'CONVEYOR', 'INTEREST_TYPES', 'FRS_FACILITY_DETAIL_REPORT_URL', 'US_MEXICO_BORDER_IND', 'COUNTY_NAME', 'HDATUM_DESC', 'LOCATION_ADDRESS', 'SITE_TYPE_NAME', 'TRIBAL_LAND_CODE', 'SIC_CODES', 'FEDERAL_AGENCY_NAME', 'SIC_CODE_DESCRIPTIONS', 'NAICS_CODES', 'CENSUS_BLOCK_CODE', 'POSTAL_CODE', 'COUNTRY_NAME', 'TRIBAL_LAND_NAME', 'COLLECT_DESC', 'SOURCE_DESC', 'SUPPLEMENTAL_LOCATION', 'CONGRESSIONAL_DIST_NUM', 'EPA_REGION_CODE', 'STATE_CODE', 'REGISTRY_ID', 'NAICS_CODE_DESCRIPTIONS'])"""

    print "test_update_iteritems()"
    print(timeit.timeit(stmt="in_line.update((k, v) for k, v in in_line.iteritems() if k in out_fields)",
                        setup=setup_statement,
                        number=2700000))
    print "test_update_items()"
    print(timeit.timeit(stmt="in_line.update((k, v) for k, v in in_line.items() if k in out_fields)",
                        setup=setup_statement,
                        number=2700000))
    print "test_dict_comp_iteritems()"
    print(timeit.timeit(stmt="{key: value for key, value in in_line.iteritems() if key in out_fields}",
                        setup=setup_statement,
                        number=2700000))
    print "test_dict_comp_items()"
    print(timeit.timeit(stmt="{key: value for key, value in in_line.items() if key in out_fields}",
                        setup=setup_statement,
                        number=2700000))
