from os.path import abspath
from random import uniform
from lists_utils import ListsUtils
from device_utils import DeviceUtils
from microservices_utils import MicroservicesApi
from files_utils import FilesUtils
import time
import json
import csv
import re


class LBUtils():
    def __init__(self):
        self.device_utils = DeviceUtils()
        self.list_utils = ListsUtils()
        self.api = MicroservicesApi()
        self.files_utils = FilesUtils()

    def get_authorization_through_api_key(self):
        """
        Used for tests that verify authorization through api key (add contact or add suppression lists entry)
        :return: Authorization
        """
        cc_id = self.api.get_contact_center_id_by_domain()
        client_credentials_token = self.api.client_credentials_token
        api_key = self.api.get_tenant_by_ccid(ccid=cc_id, token=client_credentials_token)['data']['value']['apiKey']
        return {'Authorization': 'apiKey {}'.format(api_key)}

    def job_check(self, job_id=None, timeout=None):
        """
        :param job_id: job ID for checking
        :param timeout: after wot time job check would be failed
        :return: job result
        """
        job = "RUNNING"
        job_result = "Unknown"
        trace = "None"
        max_timeout = 600  # 10 minutes timeout for performance tests
        timeout = (timeout if timeout is not None else (self.api.timeout + 10)) \
            if self.api.component != "performance_tests" else max_timeout
        timeout_except = time.time() + timeout
        start = float(time.time())
        url = "{0}/{1}/jobs/jobinstances/{2}".format(self.api.api_aggregator_uri, self.api.api_prefix, job_id)
        headers = self.api.generate_headers()
        self.api.logger.info('Waiting for job complete')
        while job != "COMPLETED":
            time.sleep(uniform(0.5, 2))
            try:
                j = self.api.session.get(url, timeout=self.api.timeout, headers=headers)
                if j.status_code == 200:
                    try:
                        if "status" in j.json()["data"]:
                            job = j.json()["data"]["status"]
                            job_result = j.json()["data"]["result"]
                            trace = j.json()["data"]["trace"]
                        elif "state" in j.json()["data"]:
                            job = j.json()["data"]["state"]
                            job_result = j.json()["data"]["result"]
                            trace = j.json()["data"]["trace"]
                        else:
                            raise Exception("Job check FAILED. Parameter [status] or [state] are missing in "
                                            "response (GET runId)")
                    except Exception as e:
                        self.api.logger.info("After GET run id: Response {0}".format(j.content))
                        raise Exception("Unable to GET job status. Exception: {0}".format(str(e)))
                else:
                    raise Exception("Get status code failed. GET {0}, Response: {1}".format(url, j.text))
            except Exception as e:
                if isinstance(e, self.api.requests.exceptions.ConnectionError):
                    time.sleep(1)
                    pass
                else:
                    raise Exception("Unable to GET job status. Exception: {0}".format(str(e)))
            if job not in ["RUNNING", "COMPLETED"]:
                raise Exception('Job status is {0}. run_id data: {1}'.format(job, j.json()['data']))
            if time.time() > timeout_except:
                raise Exception("Job is not completed after ", timeout, " seconds")

        self.api.logger.info("\r Job status is {0} after {1} seconds. Result: {2}. \nJob trace: {3}"
                             "".format(job, round((time.time() - start), 1), job_result, trace))
        return j

    def post_submitjob(self, importfile, name, listid=0, mappingfile=None, rule=None, listtype="CALLING",
                       uploadmode="CREATE", return_result=False, check_in_db=True, runid=None, job_check=True,
                       expected_code=200, csv_no_header=False, file_path=None, tz=False, selection_rule=False,
                       split_custome=False, upload_rule=None, others_count=20, other_from=None, other_to=None,
                       spc_id=None, wrong_spec=False, timeout=None, import_in_annex=True, use_spec=False,
                       data_map=None, negative_test=None, country="US"):
        """
        :param importfile: import file name
        :param name: list name
        :param listid: list ID
        :param mappingfile: specification file
        :param rule: if use rules(selection, upload, splitting)
        :param listtype: list type
        :param uploadmode: upload mode (CREATE, APPEND_ONLY, APPEND_AND_UPDATE, FLUSH_APPEND)
        :param return_result: True - return request result, False - return ID
        :param check_in_db: if need check in database
        :param runid: job run ID
        :param job_check: if need job check
        :param expected_code: expected response code. 200 == OK
        :param csv_no_header: True - if csv file without header
        :param file_path: path to csv file
        :param tz: use custom time zone
        :param selection_rule: if use selection rule
        :param split_custome: if use splitting rule
        :param upload_rule: if use upload rule
        :param others_count: count of Other fields
        :param other_from: if you need Others fields no in order(example: (from)25,...,(to)50)
        :param other_to: if you need Others fields no in order
        :param wrong_spec: for negative test for up coverage
        :param country: default "US", phone numbers country in list file (for checking in DB)
        :return: request result or ID
        """
        spec = None if not wrong_spec else "specs/"
        rule_type = "selection_rule" if listtype == "CALLING" else "selection_rule_suppression"
        url = "{0}/{1}/submit-job/".format(self.api.list_builder_host, self.api.lb_prefix)
        payload = {"listType": listtype, "listName": name, "uploadMode": uploadmode,
                   "contactsFile": 'lists/' + importfile, "specFile": spec, "listid": listid,
                   "runId": str(runid), "specificationId": spc_id, "useCustomTimezone": tz,
                   "useSpecificationFile": (True if mappingfile else use_spec)}
        if data_map:
            data = json.loads(data_map)['data']
            data_mapping = {'mappingSchemaId': data['internalId'], 'mappingSchema': data}
            payload.update(data_mapping)

        if mappingfile:
            payload.update({"specFile": 'specs/' + mappingfile})
        if rule:
            rule = json.loads(self.list_utils.get_lists(lists_type='rules', lists_id=rule).content)['data']
            if selection_rule:
                selection_name = rule['userPropertiesData']['CloudContact']['rules']['selection']
                selection_id = self.list_utils.get_list_id(type_list='rules', name=selection_name)
                condition_sets = json.loads(
                    self.list_utils.get_lists(lists_type='rules', lists_id=selection_id).content)['data'][
                    'userPropertiesData']['CloudContact']['rules']
                spc_id = rule['userPropertiesData']['CloudContact']['rules']['specFile']['DBID'] if spc_id is not None \
                    else None
                payload.update({'uploadRule': {"name": rule["name"], "id": rule["DBID"],
                                               'selection': {'name': selection_name, 'id': selection_id,
                                                             "type": rule_type, 'conditionSets': condition_sets}},
                                'specificationId': spc_id})
            elif split_custome:
                remainder = rule['userPropertiesData']['CloudContact']['rules']['remainderEnabled']
                splitting_rules = rule['userPropertiesData']['CloudContact']['rules']['splittingRules']
                spc_id = rule['userPropertiesData']['CloudContact']['rules']['specFile']['DBID']
                value = []
                for item in splitting_rules:
                    splitting_rule = json.loads(self.list_utils.get_lists(
                        lists_type='rules', lists_id=item).content)['data']
                    value.append({'name': splitting_rule['name'], 'id': splitting_rule['DBID'], "type": rule_type,
                                  'conditionSets': splitting_rule['userPropertiesData']['CloudContact']['rules']})
                payload.update({"uploadRule": {"name": rule["name"], "id": rule["DBID"],
                                               "splitting": rule['userPropertiesData']['CloudContact'][
                                                   'rules']['splitting'],
                                               'remainderEnabled': remainder,
                                               'splittingRules': value}, "listid": 0, 'specificationId': spc_id})
            elif rule['userPropertiesData']['CloudContact']['rules']['splittingUsed'] is True:
                payload.update({"uploadRule": {"name": rule["name"], "id": rule["DBID"],
                                               "splitting": rule['userPropertiesData']['CloudContact'][
                                                   'rules']['splitting']}, "listid": 0})
            else:
                payload.update({"uploadRule": {"name": rule["name"], "id": rule["DBID"]}})
        if upload_rule:
            payload.update(upload_rule)
        if negative_test:
            payload.update(negative_test)

        start_time = int(time.time())
        while int(time.time()) < start_time + self.api.timeout + 30:
            try:
                r = self.api.requests.post(url, json={"data": payload}, headers=self.api.generate_headers())
                break
            except Exception as e:
                if isinstance(e, self.api.requests.exceptions.ConnectionError):
                    time.sleep(1)
                    pass
                else:
                    self.api.logger.info('Before Request: Request to endpoint {0}/listbuilder/v2/submit-job/'
                                         ' with data: {1}'.format(self.api.list_builder_host, payload))
                    raise Exception("Unable to make POST request {}".format(str(e)))
        self.api.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if r.status_code == 200 and job_check:
            self.job_check(r.json()["data"]["id"], timeout=timeout)
        if check_in_db:

            if csv_no_header:
                table_name = 'cc_list_' + str(listid)
                expected_size = self.device_utils.get_devices_from_file(file_path)
                record = self.api.db_utils.get_records_from_db_with_parameters(table_name=table_name)
                assert len(expected_size) == len(record), "Verification in db FAILED. Expected size is {0}. " \
                                                          "Actual size: {1}".format(len(expected_size), len(record))
            else:
                self.check_import_in_db(importfile, mappingfile, listid, listtype, others_count=others_count,
                                        other_from=other_from, other_to=other_to, country=country)
        if import_in_annex and r.status_code == 200 and listtype == "CALLING" and rule is None:
            self.list_utils.conf_server.check_import_in_annex(name=name)
        if not return_result:
            return r.json()["data"]["id"]
        else:
            return r

    def delete_list(self, listid, list_type, check_in_db=True, expected_code=200, call_list=False):
        """
        :param listid: list ID
        :param list_type: list type ("list" in case of calling list)
        :param check_in_db: if need check in database
        :param expected_code: expected response code. 200 == OK
        :param call_list: True if delete calling list
        :return: response result
        """
        headers = self.api.generate_headers()
        url = "{0}/{1}/{2}/{3}".format(self.api.list_builder_host, self.api.lb_prefix, list_type, listid)
        try:
            r = self.api.requests.delete(url, headers=headers, timeout=self.api.timeout)
        except Exception as e:
            self.api.logger.info('Before Request: DELETE {0} with header: {1}'.format(url, headers))
            raise Exception("Failed to DELETE list. Exception: {0}".format(e))
        self.api.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if check_in_db:
            if call_list:   # Check tables in DB after list deletion
                table_name = "cc_list_{0}".format(listid)
                ud_table_name = "cc_list_{0}_ud".format(listid)
                not_deleted = self.api.db_utils.get_records_from_db_with_parameters(table_name=table_name)
                ud_not_deleted = self.api.db_utils.get_records_from_db_with_parameters(table_name=ud_table_name)
                assert (len(not_deleted) == len(ud_not_deleted) == 0), \
                    "Records left in {0} after list deletion: {1}. \nRecords left in {2} after list deletion: {3}" \
                    "".format(table_name, len(not_deleted), ud_table_name, len(ud_not_deleted))
            else:
                records_after_deletion = self.api.db_utils.get_records_from_db_with_parameters(
                  table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': listid})
                assert len(records_after_deletion) == 0, \
                    'Not all records are deleted. Records left with id {0} - {1}'.format(listid,
                                                                                         len(records_after_deletion))
        return r

    def export_list(self, lists_id, value_type=None, list_type='sl', compare=None, expected_code=200,
                    negative_test=False, selection_id=None):
        """
        :param lists_id: list ID
        :param value_type: expected type of exported file
        :param list_type: list type, used with compare
        :param compare: file for compare with exported file
        :param expected_code: expected response code. 200 == OK
        :param negative_test: if need negative test with invalid input values
        :param selection_id: id of selection rule for exporting not all records
        :return: export file if not negative test
        """
        headers = self.api.generate_headers()
        prefix = '?selection_rule_id={0}'.format(selection_id) if selection_id is not None else ''

        start_time = int(time.time())
        while int(time.time()) < start_time + self.api.timeout + 30:
            try:
                r = self.api.session.get("{0}/{1}/job/export/{2}/{3}{4}".format(
                    self.api.list_builder_host, self.api.lb_prefix, list_type, lists_id, prefix),
                                     timeout=self.api.timeout, headers=headers)
                break
            except Exception as e:
                if isinstance(e, self.api.requests.exceptions.ConnectionError):
                    time.sleep(1)
                    pass
                else:
                    self.api.logger.info('Before Request: Request to endpoint {0}/listbuilder/v2/job/export/'
                                         ''.format(self.api.list_builder_host))
                    raise Exception("Unable to make POST request {}".format(str(e)))
        self.api.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if not negative_test:
            export_file_name = str(json.loads(r.text)['data'])
            export_file = self.list_utils.file_utils.copy_file_from_container('export', export_file_name)

            if value_type:
                assert value_type in export_file_name, "Wrong type of list is exported, exported - {0} ," \
                                                       " expected type: {1}".format(value_type, export_file_name)
            if compare:
                if list_type == 'sl':
                    with open(export_file, 'r') as fin:
                        exported_file = fin.read().split('\n')
                    with open(compare, 'r') as c:
                        compare_file = c.read().split('\n')
                    compare_file_replaced = []
                    for line in compare_file:
                        if value_type == "client":
                            compare_file_replaced.append(line)
                        else:
                            compare_file_replaced.extend(self.device_utils.normalize_devices_in_string(line))
                    diff = set(set(compare_file_replaced) ^ set(exported_file))
                    assert diff == set([]) or diff == set(['']), \
                        'Exported file is not equal to uploaded. Difference: {0}'.format(diff)
                elif list_type == 'cl' and selection_id:
                    export_file = self.list_utils.file_utils.csv_file_to_dict(export_file)
                    assert len(export_file) == compare, \
                        'Actual count of records {0} not equal to expected {1}.'.format(len(export_file), compare)
                else:
                    records = self.api.db_utils.get_records_from_db_with_parameters(table_name="cc_list_{0}"
                                                                                    "".format(lists_id))
                    self.list_utils.file_utils.validate_count_of_records(export_file=export_file,
                                                                         records_count=len(records))
            return export_file

    def check_import_in_db(self, importfile, mappingfile, listid, listtype="CALLING", upload_mode=None, ud_before=None,
                           before=[], tablename='cc_supp_list', others_count=20, other_from=None, other_to=None,
                           imported_file=None, delimiter=",", country="US"):
        """
        :param importfile: name of import file
        :param mappingfile: specification file
        :param tablename: Table Name in DB
        :param listid: List ID
        :param listtype: List Type
        :param upload_mode: for testing import with upload modes:APPEND_ONLY, APPEND_AND_UPDATE, FLUSH_APPEND
        :param ud_before: records in UD_table before import
        :param before: records in main table before import
        :param others_count: count of Other fields
        :param other_from: if you need Others fields no in order(example: (from)25,...,(to)50)
        :param other_to: if you need Others fields no in order
        :param imported_file: for compare client IDs in before and import file, used with upload_mode
        :param delimiter: default ","; for DSV files "|"
        :param country: default "US", phone numbers country in list file
        :return: length of imported and expected records; wrong records in DB in case of upload mode
        """
        tablename = 'cc_list_{0}'.format(listid) if listtype == "CALLING" else tablename
        ud_table_name = 'cc_list_{0}_ud'.format(listid) if listtype == "CALLING" else tablename
        get_from_list = "ClientID" if listtype == "SUPPRESSION_CLIENT" else "deviceIndex"

        upload_file = self.device_utils.get_devices_or_client_from_list(abspath(
            "list_builder/files/{0}".format(importfile)), get_from_list, mappingfile,
            delimiter=delimiter, l_type=listtype, country=country)
        devices_client_before = self.device_utils.get_devices_from_list(before, list_type=listtype,
                                                                        get_from_list=get_from_list)
        ud_same_records = ud_records_after_import = before_import_ids = after_import_ids = []
        if listtype == "CALLING":
            records_after_import = self.api.db_utils.get_records_from_db_with_parameters(table_name=tablename)
            records_after_import_full = list(records_after_import)
            ud_records_after_import = self.api.db_utils.get_records_from_db_with_parameters(table_name=ud_table_name)
            ud_same_records = [item for item in ud_before if item in ud_records_after_import] if ud_before else None
            if imported_file:
                # Searching for same Client IDs in before_import table and after_import table
                import_ids = [i["ClientID"] for i in (csv.DictReader(file(imported_file)))]
                before_import_ids = [{"c_id": i["c_client_id"], "chain_id": i["chain_id"]}
                                     for i in before if i["c_client_id"] in import_ids]
                after_import_ids = [{"c_id": i["c_client_id"], "chain_id": i["chain_id"]}
                                    for i in records_after_import_full if i["c_client_id"] in import_ids]
            if not upload_mode:
                for row in before:
                    records_after_import.remove(row)
                self.list_utils.file_utils.validate_records_in_db_or_export(
                    abspath("list_builder/files/" + importfile), records_after_import,
                    ud_records=ud_records_after_import, delimiter=delimiter, country=country)
        else:
            records_after_import = self.api.db_utils.get_records_from_db_with_parameters(
                table_name=tablename, parameters_and_values={'scd_sl_uid': listid})
            records_after_import_full = list(records_after_import)
            for row in before:
                while records_after_import.count(row) > 0:
                    records_after_import.remove(row)
        expected = set(upload_file + devices_client_before) if listtype != "CALLING" else (upload_file +
                                                                                           devices_client_before)
        if upload_mode is "FLUSH_APPEND":
            # Search for not deleted old records (before import) in main and UD tables
            not_deleted = [item for item in before if item in records_after_import_full]
            assert before not in records_after_import_full, \
                "Not all records deleted from the table {0} after FLUSH_APPEND: {1}".format(tablename, not_deleted)
            if ud_before:
                assert ud_before not in ud_records_after_import, \
                    "Not deleted from the table {0} after FLUSH_APPEND: {1}".format(ud_table_name, ud_same_records)
        elif upload_mode is "APPEND_AND_UPDATE" and imported_file:
            # Search for duplicate records after import in main and UD tables
            duplicates = [item for item in after_import_ids if item in before_import_ids]
            assert duplicates == [], \
                "Duplicates in the table {0} after APPEND_AND_UPDATE: {1}".format(tablename, duplicates)
            if ud_before:
                ud_duplicates = [item for item in ud_records_after_import if item in ud_same_records]
                assert ud_duplicates == [], \
                    "Duplicates in the table {0} after APPEND_AND_UPDATE: {1}".format(ud_table_name, ud_duplicates)
        elif upload_mode is "APPEND_ONLY" and imported_file:
            # Search for updated duplicate records after import in main and UD tables
            updated_record = [item for item in before_import_ids if item not in after_import_ids]
            assert updated_record == [], \
                "UPDATED records in the table {0} after APPEND_ONLY: {1}".format(tablename, updated_record)
            if ud_before:
                ud_updated = [item for item in ud_same_records if item not in ud_records_after_import]
                assert ud_updated == [], \
                    "UPDATED records in the table {0} after APPEND_ONLY: {1}".format(ud_table_name, ud_updated)
        else:
            assert len(records_after_import_full) == len(expected), \
              "Not all records were written into the table {0}. Amount of expected records- {1}, " \
              "records after import - {2}".format(tablename, len(expected), len(records_after_import_full))

    def delete_records_from_supp_list(self, list_id, records, token=True, expected_code=200, check_in_db=True):
        headers = self.api.generate_headers() if token else None
        url = "{0}/{1}/suppression-list/{2}/records".format(self.api.list_builder_host, self.api.lb_prefix, list_id)
        records = {"records": records}
        record_from_db = self.api.db_utils.get_records_from_db_with_parameters(
            table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': list_id})
        try:
            r = self.api.requests.delete(url, headers=headers, timeout=self.api.timeout, json=records)
        except Exception as e:
            raise Exception("Unable to make DELETE request {0}".format(str(e)))
        self.api.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if check_in_db:
            supp_type = 'scd_device' if re.compile("^\+[0-9]{10,11}$").match(records['records'][0]) else 'scd_client_id'
            records_from_db = [i[supp_type] for i in record_from_db]
            result = list(set(records_from_db) & set(records))
            assert result == [], \
                'Not all records deleted from supp list. Expected count of deleted records: {0}. Actual count ' \
                'deleted records: {1}. Not deleted records: {2}'.format(len(records), len(records)-len(result), result)
        return r

    @staticmethod
    def check_others_in_exported_file(source_file, export_file, others_count=20, other_from=None, other_to=None):
        """
        :param source_file: source list file
        :param export_file: exported list file
        :param others_count: count of Other fields
        :param other_from: if you need Others fields no in order(example: (from)25,...,(to)50)
        :param other_to: if you need Others fields no in order
        :return: Other fields recorded in wrong order
        """
        export, source = [], []
        # Create list of all Other fields numbers
        all_others = range(1, others_count+1) if not (other_from or other_to) \
            else range(other_from, other_to+1) if not others_count \
            else range(1, others_count+1) + (range(other_from, other_to+1))
        try:
            # Parsing Other fields from exported file
            for records in csv.DictReader(file(export_file)):
                for other in all_others:
                    if records["other{0}".format(other)]:
                        export.append({"c_id": records["clientid"],
                                       "Other{0}".format(other): records["other{0}".format(other)]})
            # Parsing Other fields from source file
            for records in csv.DictReader(file(source_file)):
                for other in all_others:
                    if records["Other{0}".format(other)]:
                        source.append({"c_id": records["ClientID"],
                                       "Other{0}".format(other): records["Other{0}".format(other)]})
            # Search for missing Other fields or recorded in the wrong order
            wrong_export = [items for items in export if items not in source]
            wrong_source = [items for items in source if items not in export]
        except Exception as e:
            raise Exception("Wrong files for compare: {0}".format(e))
        assert wrong_export == wrong_source, "\nRecords in exported file: {0}\n not equal to\n" \
                                             "Records in source file: {1}".format(wrong_export, wrong_source)

    def check_result_of_list_splitting(self, job_id, lists_count, expected_list_size):
        lists = self.api.get_job_from_redis(job_id)["attributes"]["Lists"]
        assert len(lists) == lists_count, "List divided incorrectly, expected number of lists - {1}, " \
                                          "actual {0}".format(len(lists), lists_count)
        for item in lists:
            list_id = self.list_utils.get_list_id('contact-lists', item)
            divided_list_size = int(json.loads(self.list_utils.get_lists('list', lists_id=list_id).content)
                                    ['data']["attributes"]["size"])
            assert divided_list_size == expected_list_size, \
                "Incorrect size of list {0} in annex. Expected: {1}, Actual: {2}".format(item, expected_list_size,
                                                                                         divided_list_size)
            records = len(set(self.api.db_utils.get_records_from_db_with_parameters(
                table_name='cc_list_' + str(list_id), column_names='c_client_id')))
            assert records == expected_list_size, \
                "Incorrect size of list {0} in db. Expected: {1}, Actual: {2}".format(item, expected_list_size, records)

    def check_spec_keyword_in_db(self, **kwargs):
        keywords = {"tz": "c_tz_name", "time_zone": "c_tz_name", "timezone": "c_tz_name", "zip": "c_postal_code",
                    "zip_code": "c_postal_code", "postal_code": "c_postal_code", "zip code": "c_postal_code",
                    "postal code": "c_postal_code", "country": "c_country_code_iso",
                    "country_code": "c_country_code_iso", "country code": "c_country_code_iso",
                    "state": "c_state_code", "region": "c_state_code", "email": "contact_info",
                    "state code": "c_state_code", "statecode": "c_state_code", "state_code": "c_state_code",
                    "emailaddress": "contact_info", "workemail": "contact_info", "homeemail": "contact_info",
                    "original record": "c_original_record", "originalrecord": "c_original_record",
                    "original": "c_original_record", "original_record": "c_original_record"}
        table_name = "cc_list_{0}".format(kwargs['list_id'])
        db_records = self.api.db_utils.get_records_from_db_with_parameters(table_name=table_name)
        # Check DataBase
        if not db_records:
            raise Exception("Wrong ListID or table in DB is empty")
        from_db = [{"c_id": records["c_client_id"], k: records[v]}
                   for records in db_records for k, v in keywords.iteritems() if kwargs['keyword'] == k]
        # Check for file
        if not kwargs['file_path']:
            raise Exception("File for compare not exist")
        list_file = csv.DictReader(file("{0}/files/{1}".format(self.api.component, kwargs['file_path'])))
        from_file = [({"c_id": row["ClientID"], kwargs['keyword']: row[kwargs['keyword']]}) for row in list_file]
        wrong_in_db = [item for item in from_db if item not in from_file]
        wrong_in_file = [item for item in from_file if item not in from_db]
        assert not (wrong_in_file or wrong_in_db), "Records in DB: {0}\n not equal to\nRecords in File: " \
                                                   "{1}".format(wrong_in_db, wrong_in_file)

    def check_fields_concatenation(self, list_file, list_id, con_field_in_db, con_fields_in_list):
        """
        :param list_file: imported list file
        :param list_id: list ID in DataBase
        :param con_field_in_db: field in DataBase where concatenated fields should be recorded
        :param con_fields_in_list: [list] fields in list, which should be concatenated in field "con_field_in_db"
        :return: missing or wrong concatenated fields
        """
        fields_in_list, field_in_db = [], []
        try:
            # Parsing fields for concatenation from source file
            for records in csv.DictReader(file(list_file)):
                fields_in_list.append({records["ClientID"]: ",".join(records["{0}".format(field)]
                                                                     for field in con_fields_in_list)})
            # Parsing concatenated field from DB
            table_name = 'cc_list_{0}'.format(list_id)
            for records in self.api.db_utils.get_records_from_db_with_parameters(table_name=table_name):
                field_in_db.append({records["c_client_id"]: records[con_field_in_db]})
        except Exception as e:
            raise Exception("Wrong file or field name: {0}".format(e))
        # Search for missing or wrong concatenated fields
        wrong_in_list = [field for field in fields_in_list if field not in field_in_db]
        wrong_in_db = [field for field in field_in_db if field not in fields_in_list]
        assert wrong_in_list == wrong_in_db, "\nFields: {0} in list: {1}\n not equal to\n" \
                                             "Concatenated field: {2} in DB: {3}" \
                                             "".format(con_fields_in_list, wrong_in_list, con_field_in_db, wrong_in_db)

    def check_contact_info_type_and_cd_device_index(self, **kwargs):
        """
        :param kwargs:
        file_path: input list file with header row for compare_with_file
        list_id: list ID in DataBase
        compare_with_file: True if need compare "contact_info_type" and "cd_device_index" in list and DB
        expected_ci_type: [int] if need compare "contact_info_type" in DB with expected value
        expected_cd_index:[int] if need compare "cd_device_index" in DB with expected value
        :return: wrong mapped phone numbers
        """
        # HLD contact_info_type:
        ci_types = {"homePhone": 1, "workPhone": 2, "cellPhone": 4, "VacationPhone": 5, "VoiceMail": 8,
                    "Device1": 1, "Device2": 1, "Device3": 1, "Device4": 1, "Device5": 1,
                    "Device6": 1, "Device7": 1, "Device8": 1, "Device9": 1, "Device10": 1}
        # HLD cd_device_index:
        cd_index = {"homePhone": 2, "workPhone": 1, "cellPhone": 3, "VacationPhone": 6, "VoiceMail": 7,
                    "Device1": 1, "Device2": 2, "Device3": 3, "Device4": 4, "Device5": 5,
                    "Device6": 6, "Device7": 7, "Device8": 8, "Device9": 9, "Device10": 10}
        expected_ci_type = None if 'expected_ci_type' not in kwargs.keys() else kwargs['expected_ci_type']
        expected_cd_index = None if 'expected_cd_index' not in kwargs.keys() else kwargs['expected_cd_index']
        compare_with_file = False if 'compare_with_file' not in kwargs.keys() else kwargs['compare_with_file']
        # Check for file
        if compare_with_file and not kwargs['file_path']:
            raise Exception("File for compare not exist")
        table_name = "cc_list_{0}".format(kwargs['list_id'])
        db_records = self.api.db_utils.get_records_from_db_with_parameters(table_name=table_name)
        # Check DataBase
        if not db_records:
            raise Exception("Wrong ListID or table in DB is empty")
        # Compare "contact_info_type" in DB and expected
        if expected_ci_type is not None:
            ci_db = [{"info": rec["contact_info"], "ci_type": rec["contact_info_type"]}
                     for rec in db_records if rec["contact_info_type"] is not expected_ci_type]
            assert not ci_db, "contact_info_type in DB: {0}\n not equal to\n expected contact_info_type: {1}" \
                              "".format(ci_db, expected_ci_type)
        # Compare "cd_device_index" in DB and expected
        if expected_cd_index is not None:
            cd_db = [{"info": rec["contact_info"], "cd_index": int(rec["cd_device_index"])}
                     for rec in db_records if int(rec["cd_device_index"]) is not expected_cd_index]
            assert not cd_db, "cd_device_index in DB: {0}\n not equal to\n expected cd_device_index: {1}" \
                              "".format(cd_db, expected_cd_index)
        # Compare "contact_info_type" and "cd_device_index" in list and DB
        if compare_with_file:
            from_db = [{"c_id": records["c_client_id"], "info": records["contact_info"],
                        "ci_type": records["contact_info_type"], "cd_index": int(records["cd_device_index"])}
                       for records in db_records]
            list_file = csv.DictReader(file("{0}/files/{1}".format(self.api.component, kwargs['file_path'])))
            from_file = [({"c_id": row["ClientID"], "info": self.device_utils.normalize_device(value),
                           "ci_type": v, "cd_index": cd_index[key]}) for row in list_file
                         for key, value in row.iteritems() for k, v in ci_types.iteritems() if key == k]
            # Search for missing or wrong mapped phone numbers
            wrong_in_file = [items for items in from_file if items not in from_db]
            wrong_in_db = [items for items in from_db if items not in from_file]
            assert not (wrong_in_file or wrong_in_db), "Records in DB: {0}\n not equal to\nRecords in File: " \
                                                       "{1}".format(wrong_in_db, wrong_in_file)

    def check_cd_mask(self, file_path):
        """
        :param file_path: path to exported list file
        :return: records with wrong cd_mask_international
        """
        list_file = self.api.file_utils.csv_file_to_dict(file_path)
        cc, cd_cc, cd_mask, info = "country_code", "cd_country_code_iso", "cd_mask_international", "contact_info"
        wrong = [{"c_id": row["clientid"], info: row[info], cc: row[cc], cd_cc: row[cd_cc], cd_mask: row[cd_mask]}
                 for row in list_file if (row[cd_mask] == '1' and row[cd_cc] == row[cc])]
        assert not wrong, "Records with wrong '{}': {}".format(cd_mask, self.api.string_utils.split_messages(wrong))

    def add_contact_to_cl(self, lists_id, uploadmode='APPEND_ONLY', token=True, check_in_db=True, device=10, other=20,
                          expected_code=200, **kwargs):
        devices = []
        if token is True:
            headers = self.api.generate_headers()
        elif isinstance(token, dict):
            headers = token
        else:
            headers = None
        url = "{0}/{1}/contact/".format(self.api.list_builder_host, self.api.lb_prefix)
        if "payload" not in kwargs.keys():
            payload = {"data": {"listid": lists_id, "uploadMode": uploadmode,
                                "fields": {"FirstName": str(self.api.string_utils.fake.first_name()),
                                           "LastName": str(self.api.string_utils.fake.last_name()),
                                           "Company": str(self.api.string_utils.fake.company().replace(',', '')),
                                           "Device1": self.device_utils.rand_device(can_be_none=False),
                                           'Other1': self.api.string_utils.fake.email(),
                                           "ClientID": str(self.api.string_utils.fake.ssn()),
                                           # OCS fields
                                           "record_type": 6,
                                           "dial_sched_time": 0,
                                           "campaign_id": 101,
                                           "group_id": 102,
                                           "agent_id": "Agent Login ID up to 32 characters long"}}
                       }
            parameters = ["record_type", "dial_sched_time", "campaign_id", "group_id", "agent_id"]
            for item in parameters:
                if item not in kwargs.keys():
                    try:
                        payload['data']['fields'].pop(item)
                    except:
                        pass
            if device != 0:
                for i in range(1, device+1):
                    number = self.device_utils.rand_device(can_be_none=False)
                    devices.append(number)
                    payload['data']['fields'].update({'Device{}'.format(i): number})
            if other != 0:
                for i in range(1, other+1):
                    payload['data']['fields'].update({'Other{}'.format(i): self.api.string_utils.fake.email()})
            if kwargs:
                for key, value in kwargs.iteritems():
                    for _ in self.api.string_utils.find_in_obj(payload, key, new_value=value):
                        pass
        else:
            payload = kwargs['payload']

        start_time = int(time.time())
        while int(time.time()) < start_time + self.api.timeout + 30:
            try:
                r = self.api.requests.post(url, json=payload, headers=headers)
                break
            except Exception as e:
                if isinstance(e, self.api.requests.exceptions.ConnectionError):
                    time.sleep(1)
                    pass
                else:
                    self.api.logger.info('Before Request: Request to endpoint {0}/listbuilder/v2/contact/ with data: '
                                         '{1}'.format(self.api.list_builder_host, payload))
                    raise Exception("Unable to make POST request {0}".format(str(e)))

        self.api.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if check_in_db and r.status_code == 200:
            self.files_utils.check_in_db_add_contact_to_cl(lists_id, payload)

        return r

    def add_contact_to_supp_list(self, token=True, check_in_db=True, froms=0, expected_code=200, **kwargs):
        """
        :param token: token for authorisation
        :param check_in_db: if need check in database
        :param froms: value from, default "0" not be writen in to database
        :param expected_code: expected response code. 200 == OK
        :param kwargs:
        list_id: (int) list ID in DataBase
        list_name: (str) name of suppression list
        appendOnly: (bool) upload mode type AppendAndUpdate==False or AppendOnly==True
        client_id: (str) id of client for adding to database
        device: (str)  device of client for adding to database
        until: (int)  value till, default "0" not be writen in to database
        :return: response
        """
        append_only = True if "appendOnly" not in kwargs.keys() else kwargs['appendOnly']
        if token is True:
            headers = self.api.generate_headers()
        elif isinstance(token, dict):
            headers = token
        else:
            headers = None
        before_add = self.api.db_utils.get_records_from_db_with_parameters(
            table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': kwargs['list_id']})
        url = "{0}/{1}/suppression-list-entry".format(self.api.list_builder_host, self.api.lb_prefix)
        payload = {"data": {"listid": kwargs['list_id'],
                            "listName": kwargs['list_name'],
                            "appendOnly": True,
                            "fields": {"client_id": '',
                                       "device": '',
                                       "from": froms,
                                       "until": 0}}
                   }
        if kwargs:
            for key, value in kwargs.iteritems():
                for _ in self.api.string_utils.find_in_obj(payload, key, new_value=value):
                    pass

        payload["data"]["fields"].pop("client_id") if "client_id" not in kwargs.keys() else None
        payload["data"]["fields"].pop("device") if "device" not in kwargs.keys() else None

        start_time = int(time.time())
        while int(time.time()) < start_time + self.api.timeout + 30:
            try:
                r = self.api.requests.post(url, json=payload, headers=headers)
                break
            except Exception as e:
                if isinstance(e, self.api.requests.exceptions.ConnectionError):
                    time.sleep(1)
                    pass
                else:
                    self.api.logger.info('Before Request: Request to endpoint {0}/listbuilder/v2/suppression-list-entry'
                                         ' with data: {1}'.format(self.api.list_builder_host, payload))
                    raise Exception("Unable to make POST request {0}".format(str(e)))

        self.api.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if check_in_db and r.status_code == 200:
            value_name = 'client_id' if "client_id" in kwargs.keys() else 'device'
            after_add = self.api.db_utils.get_records_from_db_with_parameters(
                table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': kwargs['list_id']})
            before = self.list_utils.file_utils.get_all_value_from_supp_list_dict(before_add)
            after = self.list_utils.file_utils.get_all_value_from_supp_list_dict(after_add)
            added_to_db = list(set(before[value_name]) ^ set(after[value_name]))
            expected = kwargs['client_id'] if 'client_id' in kwargs.keys() \
                else self.device_utils.normalize_device(kwargs['device'])
            if append_only:
                assert len(added_to_db) == 1, "In DB supp list extra records found in database: {}".format(added_to_db)
                assert added_to_db[0] == expected, "Record added to DB supp list not equal to expected. Expected " \
                                                   "record: {0}. Record in Db: {1}".format(expected, added_to_db[0])
            else:
                assert added_to_db == [], "In DB supp list extra records found in database: {}".format(added_to_db)
            # Check 'from' and 'till' in db
            if froms > 0:
                until = payload['data']['fields']['until']
                for i in after_add:
                    if i['scd_client_id' if "client_id" in kwargs.keys() else 'scd_device'] == kwargs[value_name]:
                        assert i['scd_from'] == froms, "Incorrect value 'from' found in db for record: {0}. " \
                                                       "Expected: {1}. Actual: {2}".format(
                            kwargs[value_name], froms, i['scd_from'])
                        assert i['scd_till'] == until, "Incorrect value 'till' found in db for record: {0}. " \
                                                       "Expected: {1}. Actual: {2}".format(
                            kwargs[value_name], until, i['scd_till'])

        return r

    def find_suppression_entry(self, token=True, check_in_db=True, expected_code=200, **kwargs):

        headers = self.api.generate_headers() if token else None
        url = "{0}/{1}/suppression-lists/search".format(self.api.list_builder_host, self.api.lb_prefix)
        payload = {"data": {"device": kwargs['device'] if 'device' in kwargs.keys() else '',
                            "client_id": kwargs['client_id'] if 'client_id' in kwargs.keys() else ''}}

        payload["data"].pop("client_id") if "client_id" not in kwargs.keys() else None
        payload["data"].pop("device") if "device" not in kwargs.keys() else None

        try:
            r = self.api.requests.post(url, json=payload, headers=headers)
        except Exception as e:
            raise Exception("Unable to make POST request {0}".format(str(e)))

        self.api.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if check_in_db and r.status_code == 200:
            parameter = 'scd_client_id' if "client_id" in kwargs.keys() else 'scd_device'
            value = kwargs['device'] if 'device' in kwargs.keys() else kwargs['client_id']
            from_db = self.api.db_utils.get_records_from_db_with_parameters(table_name='cc_supp_list',
                                                                            parameters_and_values={parameter: value})
            id_from_db = [item['scd_sl_uid'] for item in from_db]
            names = [json.loads(self.list_utils.get_suppression_lists(supp_list_id=i).content)[
                         'data']['name'] for i in id_from_db]
            response = json.loads(r.content)['data']['lists']
            resp_name = [r_names['name'] for r_names in response]
            resp_id = [r_id['id'] for r_id in response]
            assert sorted(id_from_db) == sorted(resp_id), "Not all supp list id returned in response! Id's in DB: {}." \
                                                          " Returned id's: {}".format(sorted(id_from_db),
                                                                                      sorted(resp_id))
            assert sorted(names) == sorted(resp_name), "Not all supp list names returned in response! Expected names:" \
                                                       " {}. Returned names: {}.".format(sorted(names),
                                                                                         sorted(resp_name))
        return r
