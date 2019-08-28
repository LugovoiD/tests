import re
import hashlib
from api_utils.utils import *
from api_utils.microservices_utils import MicroservicesApi
from device_utils import DeviceUtils
from logger import *
from collections import OrderedDict


class ListsUtils(MicroservicesApi):
    def __init__(self):
        self.device_utils = DeviceUtils()

        super(ListsUtils, self).__init__()

    def get_lists(self, lists_type, lists_id=None, download=False, compare_with=None, auth=True, index=None,
                  expected_code=200, rules_type=None, return_unique_id=False):

        if compare_with:
            download = True
        lists_type = "list-rules" if lists_type == "rules" else "contact-lists" if lists_type == "list" else lists_type
        prefix = '/{}/download'.format(lists_id) if download and lists_type in ["contact-lists", "specifications"] \
            else '/{}'.format(lists_id) if lists_id else '?type={}'.format(rules_type) if rules_type else ''
        if index:
            prefix = '/{0}/artifacts/{1}/0/download'.format(lists_id, index)
        url = '{0}/{1}/{2}'.format(self.api_aggregator_uri, self.api_prefix, lists_type) + prefix
        cookies = self.generate_cookies(auth=auth)
        try:
            r = self.session.get(url, cookies=cookies, timeout=self.timeout)
        except Exception as e:
            raise Exception("Unable to make GET request {}".format(str(e)))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if (compare_with or index or download) and r.status_code == 200:
            try:
                unique_id = r.json()['data']['id']
                if return_unique_id:
                    return unique_id
            except Exception as e:
                raise Exception("Unable to get unique id from: {0}\nError: {1}".format(r.content, e))
            r2 = self.download_list(unique_id=unique_id, expected_code=expected_code, auth=auth, cookies=cookies)

        if compare_with and lists_type is 'specifications':
            assert hashlib.md5(r2.content).hexdigest() == self.get_md5(compare_with), \
                "Actual File's md5 - {0} is not equal to expected md5 - {1}" \
                "".format((hashlib.md5(r2.content).hexdigest()), (self.get_md5(compare_with)))

        if (download and lists_type is 'list') and r.status_code == 200:
            records = self.db_utils.get_records_from_db_with_parameters(table_name="cc_list_{}".format(lists_id))
            self.file_utils.validate_count_of_records(export_file=str(r2.text).split("\n"),
                                                      records_count=len(records) + 2)
        return r2 if ((download or index) and r.status_code == 200) else r

    def download_list(self, unique_id, expected_code=200, auth=True, cookies=None):
        url = '{0}/{1}/download/{2}'.format(self.api_aggregator_uri, self.api_prefix, unique_id)
        cookie = cookies if (auth and cookies) else {"OUTBOUND_SESSIONID": self.authorized} if auth \
            else {"OUTBOUND_SESSIONID": self.auth().json()["data"]["sessionId"]}  # new session for negative test
        try:
            #   download list by unique ID
            r = self.requests.get(url, cookies=cookie, timeout=self.timeout)
        except Exception as e:
            raise Exception("Unable to make GET request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        return r

    def delete_lists(self, lists_type, lists_id, record_id=None, auth=True, exists=True, should_exist=False,
                     check_in_cme=True, expected_code=200, check_in_db=True):
        table_name = 'cc_list_' + str(lists_id)
        record_before = self.db_utils.get_records_from_db_with_parameters(table_name=table_name)
        if exists and lists_type != 'list':
            data = self.get_lists(lists_type, lists_id).json()["data"]
        lists_type = "contact-lists" if lists_type == 'list' else "list-rules" if lists_type == "rules" else lists_type
        prefix = '/contact/{}'.format(record_id) if record_id else ''
        url = '{0}/{1}/{2}/{3}'.format(self.api_aggregator_uri, self.api_prefix, lists_type, lists_id) + prefix
        try:
            r = self.session.delete(url, cookies=self.generate_cookies(auth=auth), timeout=self.timeout,
                                    headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make DELETE request {}".format(str(e)))

        if lists_type != 'contact-lists' and check_in_cme:
            self.conf_server.check_in_cme_by_dbid(dbid=lists_id, object_type="CFGScript",
                                                  object_property="name" if exists else None,
                                                  value=data["name"] if exists else None, should_exist=should_exist)

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if check_in_db and lists_type == 'contact-lists':
            if record_id:
                record_after = self.db_utils.get_records_from_db_with_parameters(table_name=table_name)
                assert (len(record_before) - 1) == len(record_after), "Verification in db FAILED." \
                                                                      " Expected size is {0}." \
                                                                      " Actual size: {1}".format(
                    (len(record_before) - 1),
                    len(record_after))
            else:
                self.db_utils.check_table_exist(exist=False, table_name='cc_list_{}'.format(lists_id))
        return r

    def post_lists_rules_or_triggerrules(self, lists_type, name=None, rules=None, auth=True, payload=None,
                                         script_type=None, label_id=None, return_result=False, should_exist=True,
                                         expected_code=200, useVisualEditor=True, query=None):

        if auth is False:
            should_exist = False
        data = {'data': {'name': '{}'.format(name),
                         'scriptType': 'selection_rule' if script_type is None else script_type,
                         'rules': rules,
                         'useVisualEditor': useVisualEditor}}
        if not useVisualEditor:
            data['data'].update({'query': query})
        if label_id:
            data['data'].update({'label': label_id})
        try:
            r = self.session.post("{0}/{1}/list-rules".format(self.api_aggregator_uri, self.api_prefix),
                                  cookies=self.generate_cookies(auth=auth),
                                  data=payload if payload else json.dumps(data), timeout=self.timeout,
                                  headers=self.generate_csrf_headers())
        except Exception as e:
            get_logger().info('Before Request: Request to endpoint /list-rules data: {}'.format(data))
            raise Exception("Unable to make POST request {}".format(str(e)))
        self.conf_server.check_in_cme_by_name(name=name, object_type="CFGScript", should_exist=should_exist)
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if return_result:
            return r
        else:
            return r.json()["data"]["DBID"] if r.status_code == 200 else r

    def post_upload_rule(self, auth=True, name=None, expected_code=200, should_exist=True, return_result=False,
                         field_name=None, spec_file_name=True, spec_id=None, rule_type=None, value=None,
                         output_name=None, selection_name=None, splitting_used=False, remainder=False,
                         selection_id=None, return_data=False, waterfall=True, label=None, mapping_name=None,
                         data_map_id=None, script_type='upload_rule'):

        value = value if value is not None else self.string_utils.rand_range_int(1, 1, 99)
        name = name if name is not None else self.string_utils.rand_string(6)
        output_name = output_name if output_name is not None else self.string_utils.rand_string(6)
        splitting_rules = []
        field_name = field_name if field_name is not None else self.string_utils.rand_rule_fields().replace(' ',
                                                                                                            '').lower()
        data = {"data": {"name": name, "scriptType": script_type, "rules": {
            "splittingUsed": splitting_used,
            'waterfall': waterfall,
            "splittingRules": splitting_rules, "useSpecificationFile": True if spec_file_name else False}}}
        if selection_name:
            rule_set = self.get_selection_rule_set_by_name(selection_name)
            data['data']['rules'].update({'selection': selection_name, 'selectedRuleSet': rule_set})
        else:
            data['data']['rules'].update({'selection': "", 'selectedRuleSet': []})
        if spec_file_name and not isinstance(spec_file_name, bool):
            data['data']['rules'].update({"specFile": {"name": str(spec_file_name), "DBID": spec_id}})
        if mapping_name:
            data['data']['rules'].update({"specFile": None, "dataMapping": {"name": str(mapping_name),
                                                                            "internalId": data_map_id}})
        if splitting_used:
            data['data']['rules'].update({"remainderEnabled": remainder, "splitting": {}})
            if rule_type in ["splitByQuantity", "splitByPercent"]:
                data['data']['rules']['splitting'].update({"ruleType": rule_type, "value": value,
                                                           "outputName": output_name})
            elif rule_type == "splitByField":
                data['data']['rules']['splitting'].update({"ruleType": rule_type, "fieldName": field_name,
                                                           "outputName": output_name, 'label': label})
            elif rule_type == "splitByCustom":
                data['data']['rules']['splitting'].update({"ruleType": rule_type, "outputName": output_name})
                if isinstance(selection_id, list):
                    data['data']['rules'].update({'splittingRules': selection_id})
                else:
                    splitting_rules.append(selection_id)
        try:
            r = self.session.post("{0}/{1}/list-rules".format(self.api_aggregator_uri, self.api_prefix),
                                  cookies=self.generate_cookies(auth=auth), json=data, timeout=self.timeout,
                                  headers=self.generate_csrf_headers())
        except Exception as e:
            get_logger().info('Before Request: Request to endpoint /list-rules data: {}'.format(data))
            raise Exception("Unable to make POST request {}".format(str(e)))

        obj = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGScript", should_exist=should_exist)
        if splitting_used and rule_type:
            self.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter='rules', value=[rule_type])

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if return_result:
            return r
        elif return_data:
            return data if r.status_code == 200 else r
        else:
            return r.json()["data"]["DBID"] if r.status_code == 200 else r

    def put_upload_rule(self, lists_id, auth=True, name=None, expected_code=200, value=None, field_name=None,
                        spec_file_name=None, spec_id=None, rule_type=None, output_name=None, selection_name=None,
                        splitting_used=False, remainder=False, payload=None, selection_id=None, return_result=False,
                        waterfall=True, label=None):
        if name:
            payload["data"].update({"name": name})
        if selection_name:
            payload['data']['rules'].update({'selection': selection_name})
        if spec_file_name:
            payload['data']['rules'].update({"specFile": {"name": str(spec_file_name), "DBID": spec_id}})
        elif spec_file_name is None:
            payload['data']['rules'].update({'specFile': None})
        if splitting_used:
            splitting_rules = []
            payload['data']['rules'].update({"remainderEnabled": remainder,
                                             "waterfall": waterfall, "splitting": {}})
            if rule_type in ["splitByQuantity", "splitByPercent"]:
                payload['data']['rules']['splitting'].update({"ruleType": rule_type, "value": value,
                                                              "outputName": output_name})
            elif rule_type == "splitByField":
                payload['data']['rules']['splitting'].update({"ruleType": rule_type, "fieldName": field_name,
                                                              "outputName": output_name, 'label': label})
            elif rule_type == "splitByCustom":
                payload['data']['rules']['splitting'].update({"ruleType": rule_type, "outputName": output_name})
                splitting_rules.append(selection_id)

        try:
            r = self.requests.put("{0}/{1}/list-rules/{2}".format(self.api_aggregator_uri, self.api_prefix, lists_id),
                                  cookies=self.generate_cookies(auth=auth), json=payload, timeout=self.timeout,
                                  headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make PUT request {}".format(str(e)))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if return_result:
            return payload["data"]["rules"] if r.status_code == 200 else r
        else:
            return r

    def post_list(self, upload_file=None, name=None, description="", spec_id="", rule_id="", data_map_id="",
                  check_in_cme=True, auth=True, return_response=False, caller_id=None, should_exist=True,
                  job_check=True, return_object=False, expected_code=200, upload_mode=None, list_id=None,
                  empty_list=False, check_in_db=True, csv_no_header=False, tz=False, correct_list=True, label=None,
                  country="US"):
        if auth is False:
            should_exist = False
        data = {"name": name, "description": description, "specificationId": spec_id, "ruleId": rule_id,
                "dataMappingId": data_map_id,
                "type": "Standard", "useSpecificationFile": 'true' if spec_id else False} if not empty_list \
            else {"name": name, "description": description, "specificationId": "", "dataMappingId": data_map_id,
                  'attributes': {'useCustomTimezone': tz}}
        if caller_id:
            data.update({"CPNDigits": caller_id})
        if upload_mode:
            data.update({"uploadMode": upload_mode, "id": list_id})
        if spec_id is False:
            data.pop('specificationId')
        if tz:
            data.update({"useCustomTimezone": "true"})
        if label:
            data.update({"label": label})
        if data_map_id is False:
            data.pop('dataMappingId')

        start_time = int(time.time())
        while int(time.time()) < start_time + self.timeout + 30:
            try:
                if not empty_list:
                    list_file = {'listFile': (os.path.basename(upload_file), open(upload_file, 'rb'), 'text/plain')}
                    r = self.requests.post("{0}/{1}/import-contacts".format(self.api_aggregator_uri, self.api_prefix),
                                           cookies=self.generate_cookies(auth=auth), files=list_file,
                                           data=data, timeout=self.timeout, headers=self.generate_csrf_headers())
                else:
                    r = self.requests.post("{0}/{1}/contact-lists".format(self.api_aggregator_uri, self.api_prefix),
                                           cookies=self.generate_cookies(auth=auth), json={"data": data},
                                           headers=self.generate_csrf_headers())
                break
            except Exception as e:
                if isinstance(e, self.requests.exceptions.ReadTimeout):
                    time.sleep(1)
                    pass
                else:
                    get_logger().info('Before Request: Request to endpoint /import-contacts data: {}'.format(data))
                    raise Exception("Unable to make POST request {}".format(str(e)))

        if r.status_code == 500 and 'Input File not found:' in r.json()['status']['message']:
            file_name = r.json()['status']['message'].split('lists/')[1]
            get_logger().info('{}'.format(r.content))
            self.file_utils.verify_file_after_request_fail(file_type='lists', file_name=file_name)
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        resp = json.loads(r.text)
        if r.status_code == 200:
            if job_check:
                self.job_check(r.json()["data"]["internalId"])
            if check_in_db and not upload_mode:
                try:
                    table_name = 'cc_list_' + str(resp["data"]["resolvedListID"])
                    self.db_utils.check_table_exist(table_name)
                    if not csv_no_header:
                        expected_size = self.device_utils.get_devices_or_client_from_list(upload_file, country=country)
                        record = self.db_utils.get_records_from_db_with_parameters(table_name=table_name)
                        assert len(expected_size) == len(record), \
                            "Verification in db FAILED. Expected size is {0}. Actual size: {1}".format(
                                len(expected_size), len(record))
                        self.file_utils.validate_records_in_db_or_export(upload_file, record, country=country)
                        if correct_list:
                            clients = self.device_utils.get_devices_or_client_from_list(upload_file,
                                                                                        list_type="ClientID")
                            from_db = self.db_utils.get_records_from_db_with_parameters(table_name=table_name)
                            clients_db = self.device_utils.get_devices_or_client_from_dict(from_db, colling_list=True,
                                                                                           list_type="ClientID")
                            assert len(set(clients)) == len(set(clients_db)), \
                                "Verification in db FAILED. Expected size is {0}.Actual size: {1}. " \
                                "Not all clients ar writen".format(len(set(clients)), len(clients_db))
                    else:
                        expected_size = self.device_utils.get_devices_from_file(upload_file, country=country)
                        record = self.db_utils.get_records_from_db_with_parameters(table_name=table_name)
                        assert len(expected_size) == len(record), \
                            "Verification in db FAILED. Expected size is {0}. Actual size: {1}".format(
                                len(expected_size), len(record))
                except Exception as e:
                    raise Exception("Unable to verify in db. Exception: {}".format(e))
            if caller_id:
                list_imported = self.conf_server.return_object_from_cme_by_name(name=str(name),
                                                                                object_type='CFGCallingList')
                expected_caller_id = list_imported.userProperties["CloudContact"]["CPNDigits"]
                assert caller_id == expected_caller_id, "list callerID {0} is not equal" \
                                                        " to expected {1}".format(caller_id, expected_caller_id)
        if check_in_cme:
            obj = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                        should_exist=should_exist)
        if return_response:
            return r
        elif empty_list:
            return resp["data"]["id"] if r.status_code == 200 else ''
        elif return_object:
            return {"obj": obj, "id": resp["data"]["resolvedListID"]} if r.status_code == 200 else ''
        else:
            return resp["data"]["resolvedListID"] if r.status_code == 200 else ''

    def post_specifications(self, name, upload_file=None, description="", auth=True, return_response=False,
                            should_exist=True, check_md5=False, check_in_cme=True, expected_code=200, spec_file=None):

        if auth is False:
            should_exist = False
        data = {"type": "Input", "description": description, "name": name}
        spec_file = {'specfile': (os.path.basename(upload_file), open(upload_file, 'rb'), 'application/octet-stream')} \
            if spec_file is None else spec_file
        try:
            r = self.requests.post("{0}/{1}/specifications".format(self.api_aggregator_uri, self.api_prefix),
                                   data=data, files=spec_file, cookies=self.generate_cookies(auth=auth),
                                   timeout=self.timeout, headers=self.generate_csrf_headers())
        except Exception as e:
            get_logger().info('Before Request: Request to endpoint lists/specifications with data: {}'.format(data))
            raise Exception("Unable to make POST request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        time.sleep(2)
        resp = json.loads(r.text)
        data = resp["data"] if r.status_code == 200 else ''
        if check_in_cme:
            self.conf_server.check_in_cme_by_name(name=name, object_type="CFGScript", should_exist=should_exist)
        if check_md5 and r.status_code == 200:
            self.get_lists(lists_type='specifications', lists_id=data["DBID"], compare_with=upload_file)

        if return_response:
            return r
        else:
            return resp["data"]["DBID"] if r.status_code == 200 else ''

    def put_lists_rules_or_triggerrules(self, lists_type, lists_id, name=None, scripttype='selection_rule',
                                        triggerrule=None, rules=None, payload=None, auth=True, expected_code=200,
                                        label=None, use_visual_editor=True):
        lists_type = "list-rules" if lists_type == "rules" else lists_type
        if payload is None:
            payload = {"data": {"rules": rules, "useVisualEditor": use_visual_editor}}
            parameters = {'name': name, 'scriptType': scripttype, 'rules': rules, 'label': label}
            [payload['data'].update({k: v}) for k, v in parameters.iteritems() if v is not None]
        else:
            payload = {"data": {"triggerRule": triggerrule}}
            parameters = {'name': name, 'scriptType': scripttype, 'triggerRule': triggerrule}
            [payload['data'].update({k: v}) for k, v in parameters.iteritems() if v is not None]
        try:
            r = self.requests.put("{0}/{1}/{2}/{3}".format(self.api_aggregator_uri, self.api_prefix, lists_type,
                                                           lists_id),
                                  cookies=self.generate_cookies(auth=auth), json=payload, timeout=self.timeout,
                                  headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make PUT request {}".format(str(e)))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        return r

    def put_lists_list_or_specifications(self, lists_id, lists_type, upload_file=None, name=None, description=None,
                                         spec_id=None, rule_id=None, auth=True, exists=True, expected_code=200,
                                         check_md5=False, should_exist=True, tz=False, data_map_id=None,
                                         check_in_cme=True, label=None):
        if lists_type == "list":
            if upload_file:
                list_file = {'listFile': (upload_file, open(upload_file, 'rb'), 'text/plain')}
            data = {"name": name, "description": description, "specificationId": spec_id, "ruleId": rule_id,
                    "type": "Standard"} if spec_id \
                else {"data": {"DBID": lists_id, "name": name, "label": label, "specificationId": "",
                               "description": 'null' if description is None else description,
                               "dataMappingId": data_map_id,
                               'attributes': {'useCustomTimezone': tz}}}
            parameters = {'specificationId': spec_id, 'ruleId': rule_id}
            [data.update({k: v}) for k, v in parameters.iteritems() if v is not None]
            try:
                if spec_id:
                    r = self.requests.put("{0}/{1}/import-contacts".format(self.api_aggregator_uri, self.api_prefix),
                                          cookies=self.generate_cookies(auth=auth), files=list_file, data=data,
                                          headers=self.generate_csrf_headers())
                else:
                    r = self.requests.put("{0}/{1}/contact-lists/{2}".format(
                        self.api_aggregator_uri, self.api_prefix, lists_id), cookies=self.generate_cookies(auth=auth),
                        json=data, headers=self.generate_csrf_headers())
            except Exception as e:
                get_logger().info("After Request: Response {}".format(r.content))
                raise Exception("Unable to make PUT request {}".format(str(e)))
        elif lists_type is "specifications":
            if upload_file:
                specfile = {'specfile': (os.path.basename(upload_file), open(upload_file, 'rb'),
                                         'application/octet-stream')}
            payload = {'description': description}
            if name:
                payload.update({"name": name})
            try:
                r = self.requests.put("{0}/{1}/{2}/{3}".format(self.api_aggregator_uri, self.api_prefix, lists_type,
                                                               lists_id), files=specfile if upload_file else None,
                                      cookies=self.generate_cookies(auth=auth), data=payload,
                                      headers=self.generate_csrf_headers())
            except Exception as e:
                get_logger().info("After Request: Response {}".format(r.content))
                raise Exception("Unable to make PUT request {}".format(str(e)))
        else:
            raise Exception("Field data is required!")

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if exists and lists_type is "specifications":
            self.conf_server.check_in_cme_by_dbid(dbid=lists_id, object_type="CFGScript", should_exist=exists)
        if check_md5:
            self.get_lists('specifications', lists_id=lists_id, compare_with=upload_file)
        if check_in_cme and lists_type is "list":
            self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', should_exist=should_exist)

        return lists_id

    def get_list_id(self, type_list, name):
        lists = self.get_lists(type_list).json()
        for list_item in lists["data"]:
            if list_item["name"] == name:
                if type_list == "rules":
                    return list_item["DBID"]
                else:
                    return list_item["id"]

    @staticmethod
    def get_md5(filename):
        md5 = hashlib.md5(open(filename).read()).hexdigest()
        return md5

    @staticmethod
    def get_list_with_parameter(name, lists, parameter=None, value=None, exists=True):
        call_lists = lists.json()
        found_list = False
        if exists:
            for call_list in call_lists["data"] if call_lists.has_key("status") else [call_lists]:
                assert call_list.has_key("name"), "Calling list object does not have key - name, " \
                                                  "actual object is - {}".format(call_list)
                if call_list['name'] == name:
                    found_list = True
                    if value and parameter:
                        if parameter in ['size', "use", "createdDate", "fileName", "expirationDate", "specificationId"]:
                            assert value in call_list['attributes'][parameter], \
                                "Expected value {0} is not equal to actual value {1}".format(value,
                                                                                             call_list['attributes']
                                                                                             [parameter])
                        elif parameter in ["suppressionField", "suppressionMandatory"]:
                            assert value in call_list["attributes"][parameter], \
                                "Expected value {0} is not equal to actual value {1}".format(value,
                                                                                             call_list[parameter])
                        else:
                            assert call_list[parameter] == value, "Actual parameter {0} - {1} does not match " \
                                                                  "with expected result - {2}".format(parameter,
                                                                                                      call_list[
                                                                                                          parameter],
                                                                                                      value)
            if not found_list:
                raise Exception("Name - {} is not found!".format(name))
        else:
            for call_list in call_lists["data"] if call_lists.has_key("status") else [call_lists]:
                assert call_list['name'] != name, "Unexpected list '{}' found in CME".format(call_list['name'])

    def get_specification_property_from_data(self, property='DBID', name=None, dbid=None):
        specs = self.get_lists('specifications').json()
        for spec in specs['data']:
            if name:
                if name in spec['name']:
                    return spec[property]
            if dbid:
                if dbid == spec['DBID']:
                    return spec['name']

    def get_suppression_lists(self, supp_list_id=None, auth=True, expected_code=200, download=False, name=None,
                              check_size=None, index=None, return_unique_id=False):

        prefix = '/{}/download'.format(supp_list_id) if download else '/{}'.format(supp_list_id) if supp_list_id else ''
        if index:
            prefix = '/{0}/artifacts/{1}/0/download'.format(supp_list_id, index)
        url = '{0}/{1}/suppression-lists'.format(self.api_aggregator_uri, self.api_prefix) + prefix
        try:
            r = self.session.get(url, cookies=self.generate_cookies(auth=auth), timeout=self.timeout)
        except Exception as e:
            raise Exception("Unable to make GET request {}".format(e))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if (download or index) and r.status_code == 200:
            unique_id = r.json()['data']['id']
            if return_unique_id:
                return unique_id
            r2 = self.download_list(unique_id=unique_id, expected_code=expected_code, auth=auth)
            return r2

        if name and check_size:
            number = 0
            founded_name = False
            true_size = False
            for suppression_list in r.json()["data"]:
                if suppression_list['name'] == name:
                    founded_name = True
                    if str(check_size) == suppression_list['attributes']['size']:
                        true_size = True
                    else:
                        size_from_dict = suppression_list['attributes']['size']
                number += 1

            assert founded_name, 'Suppression list name {} not found'.format(name)

            assert true_size, 'incorrect size of list, actual {0}, expected {1}'.format(size_from_dict, check_size)

        return r

    def post_suppression_list(self, name, suppression_type, expiration_date, required='false', auth=True,
                              check_in_db=True, spec_id=None, expected_code=200, job_check=True, return_id=False,
                              upload_mode=None, list_id=None, csv_no_header=False, till_from=True, supp_file=None,
                              empty=False, upload_file=None, rule_id=None, country="US", channel=None):

        url = '{0}/{1}/{2}'.format(self.api_aggregator_uri, self.api_prefix,
                                   'suppression-lists' if empty else 'import-contacts')
        if upload_file:
            supp_file = {'listFile': (os.path.basename(upload_file), open(upload_file, 'rb'), 'text/plain')} \
                if supp_file is None else supp_file
        data = {"name": name, "suppressionMandatory": required, "suppressionField": suppression_type,
                "expirationDurationDays": int(expiration_date), "ruleId": rule_id,
                "specificationId": spec_id, "type": "Suppression",
                "suppressionChannels": channel if channel else ["voice"]}
        if upload_mode:
            before_append = self.db_utils.get_records_from_db_with_parameters(
                table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': list_id})
            record_db = self.device_utils.get_devices_or_client_from_dict(before_append, list_type=suppression_type)
            record_from_list = self.device_utils.get_devices_or_client_from_list(
                upload_file, l_type="sup", list_type=suppression_type, country=country)
            duplicate = list(set(record_db) & set(record_from_list))
            data.update({"uploadMode": upload_mode, "id": list_id})
            data.pop("suppressionChannels")
        if upload_mode and channel:
            data.update({"suppressionChannels": channel})
        if empty:
            [data.pop(key) for key in ['specificationId', 'type']]
            required = True if required in 'true' else False
            data.update({'suppressionMandatory': required})

        start_time = int(time.time())
        while int(time.time()) < start_time + self.timeout + 30:
            try:
                if empty:
                    r = self.requests.post(url, cookies=self.generate_cookies(auth=auth), json={"data": data},
                                           timeout=self.timeout, headers=self.generate_csrf_headers())
                else:
                    r = self.requests.post(url, cookies=self.generate_cookies(auth=auth), files=supp_file, data=data,
                                           timeout=self.timeout, headers=self.generate_csrf_headers())
                break
            except Exception as e:
                if isinstance(e, self.requests.exceptions.ReadTimeout):
                    time.sleep(1)
                    pass
                else:
                    get_logger().info('Before Request: Request to endpoint /suppression-lists with data: {}'
                                      ''.format(data))
                    raise Exception("Unable to make POST request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        resp = json.loads(r.text)
        if job_check and expected_code == 200 and not empty:
            self.job_check(r.json()["data"]["internalId"])

        if check_in_db and not empty:
            try:
                if upload_mode:
                    count = len(before_append) + len(record_from_list) - len(duplicate)
                    after_append = self.db_utils.get_records_from_db_with_parameters(
                        table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': resp["data"]["resolvedListID"]})
                    results = len(record_from_list) if upload_mode in "flushAndAppend" else count
                    assert len(after_append) == results, \
                        "Actual count of records after append: {0}. Expected count of records: {1}. " \
                        "Duplicate records is: {2}".format(len(after_append), results, duplicate)
                elif csv_no_header:
                    expected_size = self.device_utils.get_devices_from_file(upload_file, e_mails=False, country=country)
                    record = self.db_utils.get_records_from_db_with_parameters(
                        table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': resp["data"]["resolvedListID"]})
                    assert len(set(expected_size)) == len(record), "Verification in db FAILED." \
                                                                   " Expected size is {0}." \
                                                                   " Actual size: {1}".format(len(set(expected_size)),
                                                                                              len(record))
                else:
                    expected_size = self.device_utils.get_devices_or_client_from_list(
                        upload_file, suppression_type, spec_id, l_type="supp", country=country)
                    records_after_import = self.db_utils.get_records_from_db_with_parameters(
                        table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': resp["data"]["resolvedListID"]})
                    assert len(records_after_import) == len(set(expected_size)), \
                        "Count of lines {0} in imported file does not equal" \
                        " to actual count of lines in DB - {1}".format(len(set(expected_size)),
                                                                       len(records_after_import))
                    if spec_id is not None and till_from:
                        till = self.file_utils.get_from_and_till_value_from_file(upload_file)['till']
                        till_db = set(self.file_utils.get_all_value_from_supp_list_dict(records_after_import)['till'])
                        result_till = list(set(till) ^ set(till_db))
                        assert result_till == [], "Incorrect value found in DB supp list scd_till - " \
                                                  "{}".format(result_till)
                        froms = self.file_utils.get_from_and_till_value_from_file(upload_file)['from']
                        from_db = set(self.file_utils.get_all_value_from_supp_list_dict(records_after_import)['from'])
                        result_from = list(set(froms) ^ set(from_db))
                        assert result_from == [], "Incorrect value found in DB supp list scd_from - " \
                                                  "{}".format(result_from)
            except Exception as e:
                raise Exception("Unable to verify in db. Exception: {}".format(e))

        if return_id and empty:
            return resp["data"]['internalId']
        elif return_id:
            return resp["data"]["resolvedListID"]
        else:
            return resp

    def delete_suppression_list(self, supp_list_id, auth=True, clear=False, check_in_db=True, delete_records=None,
                                expected_code=200):

        records_before_delete = self.db_utils.get_records_from_db_with_parameters(
            table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': supp_list_id})
        records = {"records": delete_records}
        prefix = '/clear' if clear else '/entries' if delete_records else ''
        supp = "suppression-lists"
        url = '{0}/{1}/{2}/{3}{4}'.format(self.api_aggregator_uri, self.api_prefix, supp, supp_list_id, prefix)
        try:
            if prefix == '/clear':
                r = self.session.post(url, cookies=self.generate_cookies(auth=auth), json=records,
                                      headers=self.generate_csrf_headers())
            else:
                r = self.session.delete(url, cookies=self.generate_cookies(auth=auth), json=records,
                                        headers=self.generate_csrf_headers())
            self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        except Exception as e:
            raise Exception("Unable to make DELETE request {}".format(str(e)))

        if check_in_db:
            records_after_delete = self.db_utils.get_records_from_db_with_parameters(
                table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': supp_list_id})
            if delete_records:
                assert len(records_after_delete) == len(records_before_delete) - len(delete_records), \
                    "After delete request not all records ar deleted. Expected count of records: {0}. " \
                    "Actual count of records: {1}".format(len(records_before_delete) - len(delete_records),
                                                          len(records_after_delete))
            else:
                assert len(records_after_delete) == 0, "After delete request not all records ar deleted." \
                                                       " Actual count of line: {}".format(len(records_after_delete))
        return r

    def get_supp_list_id_or_parameter(self, name, parameter='id'):
        lists = self.get_suppression_lists().json()
        for list_item in lists["data"]:
            if list_item["name"] == name:
                if parameter in ["useCount", "size", "suppressionField", "suppressionMandatory",
                                 "expirationDurationMinutes", "expirationDurationDays"]:
                    return list_item["attributes"][parameter]
                else:
                    return list_item[parameter]

    def validate_format_of_suppression_records(self, suppression_type=None):
        column_name = 'scd_device' if suppression_type == 'deviceIndex' else 'scd_client_id'
        records_from_db = self.db_utils.get_records_from_db_with_parameters(table_name='cc_supp_list',
                                                                            column_names=column_name)
        regexp = re.compile("^\+[0-9]{10,11}$") if suppression_type is 'deviceIndex' else re.compile("[0-9]{3}-[0-9]{2}"
                                                                                                     "-[0-9]{4}$")
        incorrect_records = []
        for record in records_from_db:
            if not regexp.match(str(record)) and record is not None:
                incorrect_records.append(record)
        if incorrect_records:
            raise Exception("Following records of {0} suppression type are incorrect: {1}".format(suppression_type,
                                                                                                  incorrect_records))

    def compare_initial_file_with_records_in_db(self, file, records_in_db, records_type="deviceIndex"):
        records_in_file = list(set(file))
        if records_type == "deviceIndex":
            records_in_file = [self.device_utils.normalize_device(device) for device in records_in_file]
        if sorted(records_in_db.content.split('\n'))[0] is "":
            records = sorted(records_in_db.content.split('\n'))[1:]
        else:
            records = sorted(records_in_db.content.split('\n'))
        result = list(set(records) ^ set(records_in_file))
        assert result == [], "Initial file is not equal to downloaded. Number of records in db: {0} ," \
                             " Number of records in file: {1} .Mismatching records:" \
                             " {2}".format(len(records), len(records_in_file), result)

    @retry(tries=4, delay=1)
    def job_check(self, runid):
        job = "RUNNING"
        job_result = "?"
        timeout_except = time.time() + self.timeout + 10
        start = float(time.time())
        get_logger().info('Waiting for job complete')
        while job != "COMPLETED":
            time.sleep(1)
            j = self.session.get("{0}/{1}/jobs/jobinstances/{2}".format(self.api_aggregator_uri, self.api_prefix,
                                                                        str(runid)), timeout=self.timeout,
                                 cookies=self.generate_cookies())
            if j.status_code == 200:
                job = j.json()["data"]["state"]
                job_result = j.json()["data"]["result"]
            else:
                raise Exception("Get status code failed.Response: {} ".format(j.text))
            if job not in ["RUNNING", "COMPLETED"]:
                raise Exception('Job status is {0}. run_id data: {1}'.format(job, j.json()['data']))
            if job_result == 'FAIL':
                raise Exception('Job status is {0}. Job result is : {1}'.format(job, job_result))
            if time.time() > timeout_except:
                raise Exception("Job is not completed after {0} seconds. Result: {1}".format(self.timeout + 10,
                                                                                             job_result))

        get_logger().info(" Job is Completed after {0} seconds. Result: {1} ".format(round((time.time() - start), 2),
                                                                                     job_result))

    def check_client_id_in_db_for_calling_list_without_client_id(self, list_id, list_file, device_number='Device1'):
        try:
            table_name = 'cc_list_' + str(list_id)
            lists = self.device_utils.get_specific_devices_from_list(list_file, device_number=device_number)
            records_from_db = self.db_utils.get_records_from_db_with_parameters(table_name=table_name)
            client = []
            for record in records_from_db:
                if record["c_client_id"] is not None:
                    client.append(record["c_client_id"])

            result = list(set(lists) ^ set(client))
            assert result == [], "List without client id write in DB not correct. Actual client Id: {}".format(result)
        except Exception as e:
            raise Exception("Unable to check in DB after changes {}".format(str(e)))

    def check_supp_list_in_redis(self, name, list_file, supp_type, empty_till=False, should_exist=True, lists=None,
                                 delete_records=False, upload_mode=False, empty_from=False):
        lists = lists if lists is not None \
            else self.device_utils.get_devices_or_client_from_list(list_file, list_type=supp_type, l_type="supp")
        type = 'device=' if supp_type == "deviceIndex" else 'clientid='
        prefix = "{0}/{1}/suppression-list/{2}/entry?".format(self.list_builder_host, self.lb_prefix, name) + type
        not_found_in_redis = []
        if should_exist:
            for item in lists:
                to_from = self.device_utils.get_till_from_file(list_file, item, records_type=supp_type)
                url = prefix + ("%2B" + item.replace("+", '') if supp_type == "deviceIndex" else item)
                try:
                    r = self.requests.get(url, headers=self.generate_headers())
                except Exception as e:
                    raise Exception("Unable to make GET request {}".format(str(e)))
                from_redis = json.loads(r.content)['data'][0]
                if r.status_code == 200:
                    if empty_till:
                        assert from_redis == {"to": None, "from": None}, "Values not found in the redis. " \
                                                                         "Actual value: {}".format(from_redis)
                    if empty_from:
                        assert not from_redis["from"], "Values not found in the redis. " \
                                                       "Actual value: {}".format(from_redis)
                    else:
                        assert to_from == from_redis, "Values not found in the redis." \
                                                      " Actual value: {}".format(from_redis)
                else:
                    not_found_in_redis.append(from_redis)
            assert not_found_in_redis == [], "Expected count of record in redis: {0}. Actual count of record " \
                                             "in redis: {1}. Records not found in" \
                                             " redis: {2}".format(len(lists), len(lists) - len(not_found_in_redis),
                                                                  not_found_in_redis)
        else:
            if delete_records:
                listik = lists if upload_mode else delete_records
                not_deleted_records = []
                for item in listik:
                    url = prefix + ("%2B" + item.replace("+", '') if supp_type == "deviceIndex" else item)
                    try:
                        r = self.requests.get(url, headers=self.generate_headers())
                    except Exception as e:
                        raise Exception("Unable to make GET request {}".format(str(e)))
                    from_redis = json.loads(r.content)['data']
                    if from_redis != []:
                        not_deleted_records.append(from_redis)
                        assert item in from_redis, "Values not found in the redis. Actual value: {}".format(from_redis)
                assert not_deleted_records == [], "Not all records deleted from redis." \
                                                  " Not deleted key: {}".format(not_deleted_records)

    def check_import_activity(self, obj_cme, lists_id):
        import_activity_parameter = self.conf_server.get_import_activity_value(obj_from_cme=obj_cme)
        import_activity_value = obj_cme.userProperties["CloudContact"][import_activity_parameter[0]]
        list_info = self.get_lists("list", lists_id=lists_id)
        expected_value = list_info.json()["data"]["attributes"]["importHistory"]
        assert import_activity_value == expected_value[0], \
            "Expected value of import activity {0} from response does not equal" \
            " to value from cme {1}".format(expected_value[0], import_activity_value)

    def post_list_with_upload_rule(self, upload_file=None, name=None, description=None, spec_id=None, rule_id=None,
                                   auth=True, job_check=True, expected_code=200, tz=False, label=None, no_spec=False,
                                   data_map_id=None):
        name = name if name is not None else self.string_utils.rand_string(6)
        data = {"name": name, "ruleId": rule_id, "specificationId": spec_id, "type": "Standard",
                "dataMappingId": data_map_id,
                "useCustomTimezone": "false"}
        if spec_id is None and not no_spec:
            rule = self.get_lists('list-rules', lists_id=rule_id).json()["data"]["userPropertiesData"][
                "CloudContact"]["rules"]["specFile"]["DBID"]
            data.update({'specificationId': rule})
        if tz:
            data.update({"useCustomTimezone": "true"})
        if description:
            data.update({"description": description})
        if label:
            data.update({"label": label})
        try:
            list_file = {'listFile': (os.path.basename(upload_file), open(upload_file, 'rb'), 'text/plain')}
            r = self.requests.post("{0}/{1}/import-contacts".format(self.api_aggregator_uri, self.api_prefix),
                                   cookies=self.generate_cookies(auth=auth), files=list_file,
                                   data=data, timeout=self.timeout, headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make POST request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if job_check and r.status_code == 200:
            self.job_check(r.json()["data"]["internalId"])

        return r

    def get_selection_rule_set_by_name(self, name):
        rule = json.loads(self.conf_server.check_in_cme_by_name(name=name, object_type='CFGScript').userProperties[
                              'CloudContact']['rules'].replace("\\", ""))
        rule_set = []
        for items in rule:
            for item in items:
                set_of_rule_value = '{0} {1} {2}'.format(item['field'], item['operator'], item['value'])
                rule_set.append(set_of_rule_value)
        return rule_set

    def search_contacts(self, last_name=None, cal_list="Calling List", auth=True, expected_code=200,
                        limit=9999, offset=0, search="contacts"):
        list_id = self.get_list_id('list', cal_list)
        l_name = "LastName={0}".format(last_name) if last_name is not None else ''
        limit_offset = "&limit={0}&offset={1}".format(limit, offset) if limit is not None or offset is not None else ''
        url = '{0}/{1}/contact-lists/{2}/contacts?{3}{4}'.format(self.api_aggregator_uri, self.api_prefix, list_id,
                                                                 l_name, limit_offset)
        try:
            r = self.session.get(url, timeout=self.timeout, cookies=self.generate_cookies(auth=auth))
        except Exception:
            raise Exception("Unable to to find calling list!")
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if r.status_code == 200:
            parsed_contacts = []
            try:
                for i in r.json()["data"]["contacts"]:
                    if search == "contacts":
                        parsed_contacts.append({"c_id": i["c_client_id"], "first_name": i['c_first_name'],
                                                "last_name": i['c_last_name'], "phone_number": i['contact_info']})
                    elif search == "last_names":
                        parsed_contacts.append(i['c_last_name'])
                    elif search == "ids":
                        parsed_contacts.append(i['record_id'])
                return parsed_contacts
            except Exception as e:
                raise Exception("Missed attribute: {} in 'r.json()[data][contacts]'".format(e))
        else:
            return r

    def search_in_file_by_last_name(self, file_path, last_name=None):
        parsed_contacts = []
        for row in csv.DictReader(file(file_path)):
            last_name = last_name if last_name is not None else ""
            if last_name in row["LastName"]:
                for device_num in xrange(1, 11):
                    if row['Device{0}'.format(device_num)]:
                        try:
                            if "@" in row['Device{0}'.format(device_num)]:
                                dev_row = row['Device{0}'.format(device_num)]
                            else:
                                dev_row = self.device_utils.normalize_device(row['Device{0}'.format(device_num)])
                            parsed_contacts.append({'phone_number': dev_row, 'first_name': row['FirstName'],
                                                    'last_name': row['LastName'], 'c_id': row['ClientID']})
                        except Exception as e:
                            Exception("Device isn't a phone number: {0}".format(e))
        return parsed_contacts

    @staticmethod
    def compare_search_results(parsed_cont_list, parsed_cont_file, expected_result=True):
        try:
            wrong_records_in_file = [items for items in parsed_cont_file if items not in parsed_cont_list]
            wrong_records_in_list = [items for items in parsed_cont_list if items not in parsed_cont_file]
        except Exception as e:
            raise Exception("Wrong files for compare: {0}".format(e))
        result = True if wrong_records_in_file == wrong_records_in_list else False
        assert result == expected_result, "Compare failed.\nContact list : {0}\n not equal to" \
                                          "Source File : {1}".format(wrong_records_in_list, wrong_records_in_file)

    def put_suppression_list(self, supp_list_id, description=None, auth=True, expected_code=200, name=None,
                             required=False, expiration_date=0, supp_type=False, channel=None, **kwargs):
        payload = {"externalId": name, "description": description}
        payload.update({"expirationDurationDays": expiration_date,
                        "expirationDurationHours": kwargs["Hours"] if "Hours" in kwargs.keys() else 0,
                        "expirationDurationMinutes": kwargs["Minutes"] if "Minutes" in kwargs.keys() else 0,
                        "expirationDurationSeconds": kwargs["Seconds"] if "Seconds" in kwargs.keys() else 0,
                        "suppressionMandatory": required})
        if name:
            payload.update({"name": name})
        if supp_type:
            payload.update({"suppressionField": supp_type})
        if channel:
            payload.update({"suppressionChannels": channel})
        try:
            r = self.requests.put("{0}/{1}/suppression-lists/{2}".format(self.api_aggregator_uri, self.api_prefix,
                                                                         supp_list_id),
                                  cookies=self.generate_cookies(auth=auth), json={"data": payload},
                                  timeout=self.timeout, headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make PUT request {}".format(str(e)))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        return r

    def cometd(self, request, client_id=None, auth=True):
        if auth and not self.authorized:
            self.auth()
        if request == 'statistic connect':
            url = '{0}/{1}/statistics/notifications/connect'.format(self.api_aggregator_uri, self.api_prefix)
            data = [{"id": "476", "channel": "/meta/connect", "connectionType": "long-polling",
                     "advice": {"timeout": 0}, "clientId": "{}".format(client_id)}]

        elif request == 'handshake':
            url = '{0}/{1}/notifications/handshake'.format(self.api_aggregator_uri, self.api_prefix)
            data = [{"id": "474", "version": "1.0", "minimumVersion": "1.0", "channel": "/meta/handshake",
                     "supportedConnectionTypes": ["long-polling", "callback-polling"], "advice": {"timeout": 60000,
                                                                                                  "interval": 0}}]
        elif request == 'notifications':
            url = '{0}/{1}/notifications'.format(self.api_aggregator_uri, self.api_prefix)
            data = [{"id": "475", "channel": "/meta/subscribe", "subscription": "/service/message",
                     "clientId": "{}".format(client_id)}]
        elif request == 'statistic handshake':
            url = '{0}/{1}/statistics/notifications/handshake'.format(self.api_aggregator_uri, self.api_prefix)
            data = [{"id": "670", "version": "1.0", "minimumVersion": "1.0", "channel": "/meta/handshake",
                     "supportedConnectionTypes": ["long-polling", "callback-polling"], "advice": {"timeout": 60000,
                                                                                                  "interval": 0}}]
        elif request == 'statistic notifications':
            url = '{0}/{1}/statistics/notifications/'.format(self.api_aggregator_uri, self.api_prefix)
            data = [{"id": "671", "channel": "/meta/subscribe", "subscription": "/statistics/v3/service",
                     "clientId": "{}".format(client_id)},
                    {"id": "672", "channel": "/meta/subscribe", "subscription": "/statistics/v3/updates",
                     "clientId": "{}".format(client_id)}]
        elif request == 'connect':
            url = '{0}/{1}/notifications/connect'.format(self.api_aggregator_uri, self.api_prefix)
            data = [{"id": "477", "channel": "/meta/connect", "connectionType": "long-polling",
                     "clientId": "{}".format(client_id)}]

        try:
            r = self.session.post(url, json=data, cookies=self.session.cookies, headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception('Unable to make POST request: {}'.format(e))
        return r

    def check_contact_info_type(self, list_id, file_path, spc_file="contact_info_type", expected_result=True):
        devices = [1, 2, 4, 5, 8] if spc_file is "contact_info_type" else xrange(1, 11)
        r, from_db, from_file, wrong_records_in_db, wrong_records_in_file = 1, [], [], [], []
        try:
            for records in self.db_utils.get_records_from_db_with_parameters(table_name="cc_list_" + str(list_id)):
                if "@" not in records["contact_info"]:
                    from_db.append({"c_id": records["c_client_id"], "info": records["contact_info"],
                                    "ci_type": records["contact_info_type"]})
            for row in csv.DictReader(file(file_path)):
                for device_num in devices:
                    if row['Device{0}'.format(device_num)]:
                        try:
                            dev_row = self.device_utils.normalize_device(row['Device{0}'.format(device_num)])
                            device_num = device_num if spc_file is "contact_info_type" else 1
                            from_file.append({'c_id': row['ClientID'], 'info': dev_row, 'ci_type': device_num})
                        except Exception as e:
                            Exception("Device isn't a phone number: {0}".format(e))
            wrong_records_in_file = [items for items in from_file if items not in from_db]
            wrong_records_in_db = [items for items in from_db if items not in from_file]
        except Exception as e:
            Exception("Wrong files. {0}".format(e))
        result = True if wrong_records_in_file == wrong_records_in_db else False
        assert result == expected_result, "Compare failed.\nRecords in DB: {0}\n not equal to\nRecords in File:" \
                                          "{1}".format(wrong_records_in_db, wrong_records_in_file)

    def labels_payload(self, fields_list=None, **kwargs):
        data = {"data":
                    {"name": kwargs["label_name"] if "label_name" in kwargs.keys() else self.string_utils.rand_string(
                        8),
                     "description": kwargs["description"] if "description" in kwargs.keys() else None}}
        fields = []
        fields_count = kwargs["fields_count"] if "fields_count" in kwargs.keys() \
                                                 and isinstance(kwargs["fields_count"], int) else 20
        for i in range(fields_count, 0, -1):
            name = str("other{}".format(i))
            d = OrderedDict()
            d["name"] = name if "incorrect_name" not in kwargs.keys() else kwargs["incorrect_name"]
            d["label"] = 'Other{}'.format(i) if "label{}".format(i) not in kwargs.keys() else kwargs[
                "label{}".format(i)]
            d["display"] = True if "display{}".format(i) not in kwargs.keys() else kwargs["display{}".format(i)]
            d["attach"] = True if "attach{}".format(i) not in kwargs.keys() else kwargs["attach{}".format(i)]
            d["key"] = 'Other{}'.format(i) if "key{}".format(i) not in kwargs.keys() else kwargs["key{}".format(i)]
            if "pop_key" in kwargs.keys():
                d.pop("key")
            fields.append(d)
        data["data"].update({"fields": fields})
        if fields_list:
            return fields
        else:
            return data

    def post_labels(self, auth=True, return_response=False, expected_code=200, check_in_cme=True, **kwargs):
        data = self.labels_payload(**kwargs)
        try:
            r = self.session.post("{0}/{1}/labels".format(self.api_aggregator_uri, self.api_prefix),
                                  cookies=self.generate_cookies(auth=auth), json=data, timeout=self.timeout,
                                  headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make POST request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if check_in_cme:
            obj = self.conf_server.return_object_from_cme_by_name(name=data['data']['name'], object_type='CFGFormat')
        if return_response:
            return r
        else:
            return {'payload': data, 'DBID': r.json()["data"]["DBID"], "cme": obj if check_in_cme else ""}

    def put_labels(self, dbid, auth=True, expected_code=200, **kwargs):
        data = self.labels_payload(**kwargs)
        try:
            r = self.session.put("{0}/{1}/labels/{2}".format(self.api_aggregator_uri, self.api_prefix, dbid),
                                 cookies=self.generate_cookies(auth=auth), json=data, timeout=self.timeout,
                                 headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make PUT request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        return r

    def get_labels(self, label_name=None, dbid=None, auth=True, expected_code=200):
        prefix = '/{}'.format(dbid) if dbid is not None else ''
        url = "{0}/{1}/labels".format(self.api_aggregator_uri, self.api_prefix) + prefix
        try:
            r = self.session.get(url, timeout=self.timeout, cookies=self.generate_cookies(auth=auth))
        except Exception as e:
            raise Exception('Unable to make GET request: {}'.format(e))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if label_name:
            return r.json()["data"]["name"]
        else:
            return r

    def delete_labels(self, dbid=[], auth=True, expected_code=200, attached_labels=False):
        data = {"data": {"ids": dbid}}
        try:
            if attached_labels is True:
                r = self.session.delete("{0}/{1}/labels".format(self.api_aggregator_uri, self.api_prefix),
                                        cookies=self.generate_cookies(auth=auth), timeout=self.timeout, json=data,
                                        headers=self.generate_csrf_headers())
            else:
                r = self.session.delete("{0}/{1}/labels/{2}".format(self.api_aggregator_uri, self.api_prefix, dbid),
                                        cookies=self.generate_cookies(auth=auth), timeout=self.timeout, json=data,
                                        headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make DELETE request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        return r

    @staticmethod
    def annex_for_default_label():
        annex = {'CloudContact': {
            'fileds': '[{"name":"other1","label":"Other1","display":true,"attach":true,"key":"Other1"},'
                      '{"name":"other2","label":"Other2","display":true,"attach":true,"key":"Other2"},'
                      '{"name":"other3","label":"Other3","display":true,"attach":true,"key":"Other3"},'
                      '{"name":"other4","label":"Other4","display":true,"attach":true,"key":"Other4"},'
                      '{"name":"other5","label":"Other5","display":true,"attach":true,"key":"Other5"},'
                      '{"name":"other6","label":"Other6","display":true,"attach":true,"key":"Other6"},'
                      '{"name":"other7","label":"Other7","display":true,"attach":true,"key":"Other7"},'
                      '{"name":"other8","label":"Other8","display":true,"attach":true,"key":"Other8"},'
                      '{"name":"other9","label":"Other9","display":true,"attach":true,"key":"Other9"},'
                      '{"name":"other10","label":"Other10","display":true,"attach":true,"key":"Other10"},'
                      '{"name":"other11","label":"Other11","display":true,"attach":true,"key":"Other11"},'
                      '{"name":"other12","label":"Other12","display":true,"attach":true,"key":"Other12"},'
                      '{"name":"other13","label":"Other13","display":true,"attach":true,"key":"Other13"},'
                      '{"name":"other14","label":"Other14","display":true,"attach":true,"key":"Other14"},'
                      '{"name":"other15","label":"Other15","display":true,"attach":true,"key":"Other15"},'
                      '{"name":"other16","label":"Other16","display":true,"attach":true,"key":"Other16"},'
                      '{"name":"other17","label":"Other17","display":true,"attach":true,"key":"Other17"},'
                      '{"name":"other18","label":"Other18","display":true,"attach":true,"key":"Other18"},'
                      '{"name":"other19","label":"Other19","display":true,"attach":true,"key":"Other19"},'
                      '{"name":"other20","label":"Other20","display":true,"attach":true,"key":"Other20"}]',
            'default': 'true',
            'fields': '[{"name":"other20","label":"Other20","display":true,"attach":true,"key":"Other20"},'
                      '{"name":"other19","label":"Other19","display":true,"attach":true,"key":"Other19"},'
                      '{"name":"other18","label":"Other18","display":true,"attach":true,"key":"Other18"},'
                      '{"name":"other17","label":"Other17","display":true,"attach":true,"key":"Other17"},'
                      '{"name":"other16","label":"Other16","display":true,"attach":true,"key":"Other16"},'
                      '{"name":"other15","label":"Other15","display":true,"attach":true,"key":"Other15"},'
                      '{"name":"other14","label":"Other14","display":true,"attach":true,"key":"Other14"},'
                      '{"name":"other13","label":"Other13","display":true,"attach":true,"key":"Other13"},'
                      '{"name":"other12","label":"Other12","display":true,"attach":true,"key":"Other12"},'
                      '{"name":"other11","label":"Other11","display":true,"attach":true,"key":"Other11"},'
                      '{"name":"other10","label":"Other10","display":true,"attach":true,"key":"Other10"},'
                      '{"name":"other9","label":"Other9","display":true,"attach":true,"key":"Other9"},'
                      '{"name":"other8","label":"Other8","display":true,"attach":true,"key":"Other8"},'
                      '{"name":"other7","label":"Other7","display":true,"attach":true,"key":"Other7"},'
                      '{"name":"other6","label":"Other6","display":true,"attach":true,"key":"Other6"},'
                      '{"name":"other5","label":"Other5","display":true,"attach":true,"key":"Other5"},'
                      '{"name":"other4","label":"Other4","display":true,"attach":true,"key":"Other4"},'
                      '{"name":"other3","label":"Other3","display":true,"attach":true,"key":"Other3"},'
                      '{"name":"other2","label":"Other2","display":true,"attach":true,"key":"Other2"},'
                      '{"name":"other1","label":"Other1","display":true,"attach":true,"key":"Other1"}]',
            'scriptType': 'label'}}

        return annex

    @staticmethod
    def get_list_id_and_name_from_dict(data):
        list_id = []
        name = []
        data = data.json()["data"]
        for item in data:
            if "id" in item:
                list_id.append(item["id"])
            if "name" in item:
                name.append(item["name"])
        return {"id": list_id, "name": name}

    def post_data_mapping(self, name, auth=True, expected_code=200, should_exist=True, return_response=False,
                          data_mapping=None):
        """
        :param name: name data mapping
        :param auth: authorization default == True.
        :param expected_code: expected HTTP codes ("200", "500")
        :param should_exist: check created data mapping in cme
        :param data_mapping: payload for create data mapping
        :return: id created data mapping or response
        """
        if auth is False:
            should_exist = False
        try:
            r = self.requests.post("{0}/{1}/mapping-schemas".format(self.api_aggregator_uri, self.api_prefix),
                                   cookies=self.generate_cookies(auth=auth), json=data_mapping,
                                   headers=self.generate_csrf_headers())
        except Exception as e:
            get_logger().info('Before Request: Request to endpoint lists/mapping-schemas: {}'.format(data_mapping))
            raise Exception("Unable to make POST request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        self.conf_server.check_in_cme_by_name(name=name, object_type='CFGScript', should_exist=should_exist)
        if return_response:
            return r
        else:
            return {"id": r.json()["data"]["internalId"] if r.status_code == 200 else r}

    def get_data_mapping(self, data_id=None, auth=True, expected_code=200):
        prefix = '/{0}'.format(data_id) if data_id is not None else ''
        url = '{0}/{1}/mapping-schemas'.format(self.api_aggregator_uri, self.api_prefix) + prefix
        try:
            r = self.session.get(url, cookies=self.generate_cookies(auth=auth), timeout=self.timeout)
        except Exception as e:
            get_logger().info('Before Request: Request to '
                              'endpoint lists/mapping-schemas: {}'.format(self.api_aggregator_uri))
            raise Exception("Unable to make GET request {}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        return r.json() if r.status_code == 200 else r

    def delete_data_mapping(self, data_id=None, auth=True, check_in_cme=True, expected_code=200):
        try:
            r = self.session.delete("{0}/{1}/mapping-schemas/{2}".format(self.api_aggregator_uri, self.api_prefix,
                                                                         data_id), timeout=self.timeout,
                                    cookies=self.generate_cookies(auth=auth), headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make DELETE request {0}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if check_in_cme:
            self.conf_server.check_in_cme_by_dbid(dbid=data_id, object_type='CFGScript', should_exist=False)
        return r.json() if r.status_code == 200 else r

    def put_data_mapping(self, data_id, data_mapping=None, auth=True, expected_code=200, return_result=False):

        try:
            r = self.requests.put(
                "{0}/{1}/mapping-schemas/{2}".format(self.api_aggregator_uri, self.api_prefix, data_id),
                cookies=self.generate_cookies(auth=auth), json=data_mapping, timeout=self.timeout,
                headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make PUT request {}".format(str(e)))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if return_result:
            return data_mapping["data"] if r.status_code == 200 else r
        else:
            return r

    def generate_data_mapping(self, header, name=True, **kwargs):
        """
        :param header: header line from list file
        :param name: for PUT request this field doesn't needed
        :param kwargs:
            types: [types] mappingFields 'type' according to header fields
            unique: mappingFields 'unique' parameter for all fields
            strict: mappingFields 'strict' parameter for all fields
            readOnly: mappingFields 'readOnly' parameter for all fields
        :return: data mapping payload
        """

        data = {"data":
                    {"internalId": None,
                     "name": name,
                     "description": None,
                     "type": "import",
                     "attributes":
                         {"mappingType": "delimited",
                          "mappingSource": "fieldNumber",
                          "mappingRecord": 1,
                          "delimiter": ",",
                          "numberOfHeaderRecords": 1,
                          "numberOfTrailerRecords": 0,
                          "labelSet": None,
                          "errorLimitPercent": 0,
                          "errorLimit": 0,
                          "creditCardCheck": False,
                          "callerID": None,
                          "fixedWidth": None},
                     "mappingFields": []}}
        if name is False:
            data["data"].pop("name")

        if kwargs is not None:
            for key, value in kwargs.iteritems():
                for _ in self.string_utils.find_in_obj(data, key, new_value=value):
                    pass

        header_row = header if isinstance(header, list) else header.split(',')
        unique = False if 'unique' not in kwargs.keys() else kwargs['unique']
        strict = False if 'strict' not in kwargs.keys() else kwargs['strict']
        read_only = None if 'readOnly' not in kwargs.keys() else kwargs['readOnly']
        mapping_fields_types = ["string"] * len(header_row) if 'types' not in kwargs.keys() else kwargs['types']
        types = mapping_fields_types if isinstance(mapping_fields_types, list) else mapping_fields_types.split(',')
        # Possible Fields(in Header): Labels(Contact fields)
        contact_fields = {"FirstName": "firstname", "LastName": "lastname", "Company": "companyname",
                          "ClientID": "clientid", "homePhone": "homephone", "workPhone": "workphone",
                          "VacationPhone": "vacationphone", "VoiceMail": "voicemail", "cellPhone": "mobilephone",
                          'email': 'email', "emailaddress": "email", "homeemail": "email", "workemail": "workemail",
                          "OriginalRecord": "originalrecord"}
        contact_fields.update({"Device{}".format(i): "device{}".format(i) for i in range(1, 11)})
        contact_fields.update({"Other{}".format(i): "other{}".format(i) for i in range(1, 251)})
        # Generate 'mappingFields' for Delimited or Fixed
        if data["data"]["attributes"]["mappingType"] == "delimited":
            mapping_fields = [{"name": field, "type": types[idx], "number": idx + 1, "startPos": None,
                               "readOnly": read_only, "fieldLength": None, "unique": unique, "strict": strict,
                               "contactField": contact_fields[field]} for idx, field in enumerate(header_row)]
        else:
            values_length = kwargs['values_length']
            [field.append(contact_fields[header_row[idx]]) for idx, field in enumerate(values_length)]
            mapping_fields = [{"name": None, "type": types[idx], "number": None, "startPos": field[0],
                               "readOnly": read_only, "fieldLength": field[1], "unique": unique, "strict": strict,
                               "contactField": field[2]} for idx, field in enumerate(values_length)]

        # if data["data"]["attributes"]["mappingSource"] != "fieldName":
        #     [i.pop("name") for i in mapping_fields]
        data["data"]["mappingFields"] = mapping_fields
        return data

    def add_contact_to_cl(self, lists_id, uploadmode='APPEND_ONLY', auth=True, check_in_db=True, device=10, other=20,
                          expected_code=200, **kwargs):
        devices = []
        url = "{0}/{1}/contact-lists/{2}/contacts".format(self.api_aggregator_uri, self.api_prefix, lists_id)
        payload = {"data": {"uploadMode": uploadmode,
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Company": str(self.string_utils.fake.company().replace(',', '')),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       'Other1': self.string_utils.fake.email(),
                                       "ClientID": str(self.string_utils.fake.ssn())}}
                   }
        if device != 0:
            for i in range(1, device + 1):
                number = self.device_utils.rand_device(can_be_none=False)
                devices.append(number)
                payload['data']['fields'].update({'Device{}'.format(i): number})
        if other != 0:
            for i in range(1, other + 1):
                payload['data']['fields'].update({'Other{}'.format(i): self.string_utils.fake.email()})
        if kwargs:
            for key, value in kwargs.iteritems():
                for _ in self.string_utils.find_in_obj(payload, key, new_value=value):
                    pass

        try:
            r = self.session.post(url, cookies=self.generate_cookies(auth=auth), json=payload, timeout=self.timeout,
                                  headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make POST request {0}".format(str(e)))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if check_in_db and r.status_code == 200:
            self.file_utils.check_in_db_add_contact_to_cl(lists_id, payload)

        return r

    def filtering_rule_payload(self, **kwargs):
        order_fields = ["firstname", "lastname", "clientid", "companyname", "timezone", "postal_code", "country_code",
                        "state", "originalrecord", "device1", "device2", "device3", "device4", "device5", "device6",
                        "device7", "device8", "device9", "device10", "other1", "other2", "other3", "other4", "other5",
                        "other6", "other7", "other8", "other9", "other10", "other11", "other12", "other13", "other14",
                        "other15", "other16", "other17", "other18", "other19", "other20"]
        payload = {"data": {
            "name": kwargs["name"] if "name" in kwargs.keys() else self.string_utils.rand_string(6),
            "scriptType": "filtering_rule" if "scriptType" not in kwargs.keys() else kwargs["scriptType"],
            "label": kwargs["label"] if "label" in kwargs.keys() else None,
            "rules": {"selection": kwargs["selection"] if "selection" in kwargs.keys() else ""}}}
        if "query" in kwargs.keys():
            payload['data']['rules'].update({"useVisualEditor": False, "query": kwargs["query"]})
        elif "ascDesc" in kwargs.keys():
            payload['data']['rules'].update({"useVisualEditor": True, "ascDesc": kwargs["ascDesc"]})
        elif "useVisualEditor" in kwargs.keys():
            payload['data']['rules'].update({"useVisualEditor": kwargs["useVisualEditor"]})
            if kwargs["useVisualEditor"] is True:
                ordering_count = kwargs["ordering_count"] if "ordering_count" in kwargs.keys() else randint(1, 39)
                order_range = sample(order_fields, ordering_count)
                asc_desc = [{"field": field, "sort": choice(["asc", "desc"])} for field in order_range]
                payload['data']['rules'].update({'ascDesc': asc_desc})
        return payload

    def post_filtering_rule(self, request='post', auth=True, return_response=False, expected_code=200,
                            check_in_cme=True, dbid=None, **kwargs):
        data = kwargs["payload"] if "payload" in kwargs.keys() else self.filtering_rule_payload(**kwargs)
        try:
            if request == 'post':
                r = self.requests.post("{0}/{1}/list-rules".format(self.api_aggregator_uri, self.api_prefix),
                                       cookies=self.generate_cookies(auth=auth), json=data, timeout=self.timeout,
                                       headers=self.generate_csrf_headers())
            if request == 'put' and dbid:
                r = self.requests.put("{0}/{1}/list-rules/{2}".format(self.api_aggregator_uri, self.api_prefix, dbid),
                                      cookies=self.generate_cookies(auth=auth), json=data, timeout=self.timeout,
                                      headers=self.generate_csrf_headers())
            if request == 'delete' and dbid:
                r = self.requests.delete(
                    "{0}/{1}/list-rules/{2}".format(self.api_aggregator_uri, self.api_prefix, dbid),
                    cookies=self.generate_cookies(auth=auth), json=data, timeout=self.timeout,
                    headers=self.generate_csrf_headers())
        except Exception as e:
            raise Exception("Unable to make {0} request {1}".format(request.upper(), str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        if check_in_cme:
            obj = self.conf_server.return_object_from_cme_by_name(name=data['data']['name'], object_type='CFGScript')
        if return_response:
            return r
        else:
            return {'payload': data, 'DBID': r.json()["data"]["DBID"] if request == 'post' else None,
                    "cme": obj if check_in_cme else ""}

    def add_contact_to_supp_list(self, auth=True, check_in_db=True, froms=0, expected_code=200, **kwargs):
        """
        :param auth: auth for authorisation
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
        before_add = self.db_utils.get_records_from_db_with_parameters(
            table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': kwargs['list_id']})
        url = "{0}/{1}/suppression-lists/entry".format(self.api_aggregator_uri, self.api_prefix)
        payload = {"data": {"listid": kwargs['list_id'],
                            "listName": kwargs['list_name'],
                            "appendOnly": True,
                            "fields": {"client_id": '',
                                       "device": '',
                                       "from": froms,
                                       "until": 86400}}
                   }
        if kwargs:
            for key, value in kwargs.iteritems():
                for _ in self.string_utils.find_in_obj(payload, key, new_value=value):
                    pass

        payload["data"]["fields"].pop("client_id") if "client_id" not in kwargs.keys() else None
        payload["data"]["fields"].pop("device") if "device" not in kwargs.keys() else None

        try:
            r = self.session.post(url, cookies=self.generate_cookies(auth=auth), json=payload, timeout=self.timeout,
                                  headers=self.generate_csrf_headers())
        except Exception as e:
            get_logger().info('Before Request: Request to endpoint {0}suppression-list/id/entry with '
                              'data: {1}'.format(self.api_aggregator_uri, payload))
            raise Exception("Unable to make POST request {0}".format(str(e)))

        self.string_utils.assert_status_code(r=r, expected_code=expected_code)

        if check_in_db and r.status_code == 200:
            value_name = 'client_id' if "client_id" in kwargs.keys() else 'device'
            after_add = self.db_utils.get_records_from_db_with_parameters(
                table_name='cc_supp_list', parameters_and_values={'scd_sl_uid': kwargs['list_id']})
            before = self.file_utils.get_all_value_from_supp_list_dict(before_add)
            after = self.file_utils.get_all_value_from_supp_list_dict(after_add)
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

    def delete_multiple_records_from_list(self, list_id, records, auth=True, expected_code=200):
        """
        :param list_id: (int) list ID in DataBase
        :param records: records for delete from list
        :param auth:  auth for authorisation
        :param expected_code: expected response code. 200 == OK
        :return:
        """
        payload = {"data": {"records": records}}
        try:
            r = self.requests.delete("{0}/{1}/contact-lists/{2}/records".format(
                self.api_aggregator_uri, self.api_prefix, list_id),
                headers=self.generate_csrf_headers(), cookies=self.generate_cookies(auth=auth),
                timeout=self.timeout, json=payload)
        except Exception as e:
            raise Exception("Unable to make DELETE request {0}".format(str(e)))
        self.string_utils.assert_status_code(r=r, expected_code=expected_code)
        return r
