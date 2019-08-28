from re import compile
from api_utils.outbound_test_case import OutboundBaseTestApi
from api_utils.settings_utils import SettingsUtils
from api_utils.lb_utils import LBUtils
from api_utils.lm_utils import LMUtils
from api_utils.lists_utils import ListsUtils
from api_utils.compliance_utils import ComplianceUtils
from ddt import ddt, data, unpack
from os.path import basename
import json
from api_utils.utils import get_csv_data


@ddt
class TestCallingLists(OutboundBaseTestApi):
    @classmethod
    def setUpClass(cls):
        super(TestCallingLists, cls).setUpClass()
        cls.conf_server = cls.api.conf_server
        cls.files_utils = cls.api.file_utils
        cls.db_utils = cls.api.db_utils
        cls.settings_utils = SettingsUtils()
        cls.compliance_utils = ComplianceUtils()
        cls.lb_utils = LBUtils()
        cls.lm_utils = LMUtils()
        cls.lists_utils = ListsUtils()
        # Settings are needed to check the correctness of filling some values in dB for some tests
        cls.set_id = cls.settings_utils.get_settings(return_id=True)
        cls.payload_settings = cls.settings_utils.settings_payload(countryCode="US", timeZone="America/New_York",
                                                                   default_err_tz="Europe/London", max_lists_split=11,
                                                                   default_region="CA")
        cls.settings_utils.put_settings(payload=cls.payload_settings, set_id=cls.set_id)
        cls.list_file = cls.files_utils.make_file("calling list", name='calling_list', records_count=50)
        cls.spec_file = 'list_builder/files/list_spec.spc'
        cls.cl_for_upload_file = 'list_builder/files/cl_for_upload_rule.txt'
        cls.cl_for_rules = cls.files_utils.make_advanced_list_file(records=100, devices_correct=True, extension="csv",
                                                                   name="cl_for_rules")
        cls.cl_for_rules2 = 'list_builder/files/list_for_rules_d_types.csv'
        # Copy files to container
        copy_list = [cls.list_file, cls.cl_for_upload_file, cls.cl_for_rules, cls.cl_for_rules2]
        [cls.files_utils.copy_file_to_container('lists', item) for item in copy_list]
        cls.files_utils.copy_file_to_container('specs', cls.spec_file)

        cls.spc_name = 'spc_for_lb'
        cls.spec_id = cls.lists_utils.post_specifications(name=cls.spc_name, upload_file=cls.spec_file)
        cls.name_f_name_equal = 'f_name_equal'
        cls.name_f_name_not_equal = 'f_name_not_equal'
        cls.name_l_name_equal = 'l_name_equal'
        cls.name_company_equal = 'company_equal'
        cls.name_company_not_equal = 'company_not_equal'
        cls.f_name_equal = cls.files_utils.generate_rule_list(fields=["firstname"], types=["string"],
                                                              operators=["equal"], values=["Alex"])
        cls.f_name_not_equal = cls.files_utils.generate_rule_list(fields=["firstname"], types=["string"],
                                                                  operators=["not equal"], values=["Alex"])
        l_name_equal = cls.files_utils.generate_rule_list(fields=["lastname"], types=["string"], operators=["equal"],
                                                          values=["Dominguez"])
        cls.company_equal = cls.files_utils.generate_rule_list(fields=["companyname"], types=["string"],
                                                               operators=["equal"], values=["Asfero"])
        cls.company_not_equal = cls.files_utils.generate_rule_list(fields=["companyname"], types=["string"],
                                                                   operators=["not equal"], values=["Asfero"])
        cls.selection_f_name_id = cls.lists_utils.post_lists_rules_or_triggerrules(
            lists_type='rules', name=cls.name_f_name_equal, rules=[cls.f_name_equal])
        cls.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=cls.name_f_name_not_equal,
                                                         rules=[cls.f_name_not_equal])
        cls.selection_l_name_id = cls.lists_utils.post_lists_rules_or_triggerrules(
            lists_type='rules', name=cls.name_l_name_equal, rules=[l_name_equal])
        cls.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=cls.name_company_equal,
                                                         rules=[cls.company_equal])
        cls.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=cls.name_company_not_equal,
                                                         rules=[cls.company_not_equal])
        cls.cl_for_rules_id = cls.lm_utils.post_list("list", basename(cls.cl_for_rules),
                                                     name='cl_for_rules')['id']
        cls.cl_for_rules2_id = cls.lm_utils.post_list("list", basename(cls.cl_for_rules2),
                                                      name='cl_for_rules2')['id']

    def test_01_post_list_with_splitting_rule_splitByQuantity(self):
        name = "test_01_cl_with_upload_rules"
        records_count = 50
        quantity = records_count / 5
        list_file = self.files_utils.make_file("calling list", name=name, records_count=records_count)
        self.files_utils.copy_file_to_container('lists', list_file)
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, value=quantity,
                                                    spec_file_name=self.spc_name, splitting_used=True,
                                                    rule_type='splitByQuantity', output_name=name + '_%d')
        job_id = self.lb_utils.post_submitjob(importfile=basename(list_file),
                                              mappingfile=basename(self.spec_file), name=name,
                                              rule=rule_id, check_in_db=False)
        self.lb_utils.check_result_of_list_splitting(job_id=job_id, lists_count=5, expected_list_size=quantity)

    def test_02_post_list_with_splitting_rule_splitByPercent(self):
        name = "test_02_cl_with_upload_rules"
        records_count = 50
        quantity = records_count / 5
        list_file = self.files_utils.make_file("calling list", name=name, records_count=records_count)
        self.files_utils.copy_file_to_container('lists', list_file)
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, value='20',
                                                    spec_file_name=self.spc_name, splitting_used=True,
                                                    rule_type='splitByPercent', output_name=name + '_%d')
        job_id = self.lb_utils.post_submitjob(importfile=basename(list_file),
                                              mappingfile=basename(self.spec_file), name=name,
                                              rule=rule_id, check_in_db=False)
        self.lb_utils.check_result_of_list_splitting(job_id=job_id, lists_count=5, expected_list_size=quantity)

    def test_03_post_list_with_splitting_rule_splitByField(self):
        name = "test_03_cl_with_upload_rules"
        records_count = 10
        quantity = records_count / 10
        list_file = self.files_utils.make_file("calling list", name=name, records_count=records_count)
        self.files_utils.copy_file_to_container('lists', list_file)
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, field_name="ClientID",
                                                    spec_file_name=self.spc_name, splitting_used=True,
                                                    rule_type='splitByField', output_name=name + '_%d')
        job_id = self.lb_utils.post_submitjob(importfile=basename(list_file),
                                              mappingfile=basename(self.spec_file), name=name,
                                              rule=rule_id, check_in_db=False)
        self.lb_utils.check_result_of_list_splitting(job_id=job_id, lists_count=10, expected_list_size=quantity)

    def test_04_post_list_with_upload_rule_and_selection_rule_first_name_equal(self):
        name = "test_04_cl_with_upload_rules"
        records_count = 10
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=self.name_f_name_equal)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_05_post_list_with_upload_rule_and_selection_rule_first_name_not_equal(self):
        name = "test_05_cl_with_upload_rules"
        records_count = 40
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[self.f_name_not_equal])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_06_post_list_with_upload_rule_and_selection_rule_last_name_equal(self):
        name = "test_06_cl_with_upload_rules"
        records_count = 10
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=self.name_l_name_equal)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_07_post_list_with_upload_rule_and_selection_rule_last_name_not_equal(self):
        name = "test_07_cl_with_upload_rules"
        records_count = 40
        rule = self.files_utils.generate_rule_list(fields=["lastname"], types=["string"], operators=["not equal"],
                                                   values=["Dominguez"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_08_post_list_with_upload_rule_and_selection_rule_company_equal(self):
        name = "test_08_cl_with_upload_rules"
        records_count = 10
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[self.company_equal])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_09_post_list_with_upload_rule_and_selection_rule_company_not_equal(self):
        name = "test_09_cl_with_upload_rules"
        records_count = 40
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[self.company_not_equal])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_10_post_list_with_upload_rule_and_selection_rule_other_equal(self):
        name = "test_10_cl_with_upload_rules"
        records_count = 2
        rule = self.files_utils.generate_rule_list(fields=["other1"], types=["string"], operators=["equal"],
                                                   values=["Asfero@gmail.com"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_11_post_list_with_upload_rule_and_selection_rule_other_not_equal(self):
        name = "test_11_cl_with_upload_rules"
        records_count = 48
        rule = self.files_utils.generate_rule_list(fields=["other1"], types=["string"], operators=["not equal"],
                                                   values=["Asfero@gmail.com"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_12_post_list_with_upload_rule_and_selection_rule_client_id_equal(self):
        name = "test_12_cl_with_upload_rules"
        records_count = 1
        rule = self.files_utils.generate_rule_list(fields=["clientid"], types=["numeric"], operators=["equal"],
                                                   values=["999999999"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_13_post_list_with_upload_rule_and_selection_rule_client_id_not_equal(self):
        name = "test_13_cl_with_upload_rules"
        records_count = 49
        rule = self.files_utils.generate_rule_list(fields=["clientid"], types=["numeric"], operators=["not equal"],
                                                   values=["999999999"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_14_post_list_with_upload_rule_and_selection_rule_client_id_custom_js(self):
        name = "test_14_cl_with_upload_rules"
        records_count = 5
        rule = self.files_utils.generate_rule_list(fields=["clientid"], types=["numeric"],
                                                   operators=["Custom JS Expression"], values=["${value} > 5000"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_15_post_list_with_upload_rule_and_selection_rule_client_id_string_equal(self):
        name = "test_15_cl_with_upload_rules"
        records_count = 1
        rule = self.files_utils.generate_rule_list(fields=["clientid"], types=["string"],
                                                   operators=["equal"], values=["Higgins"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_16_post_list_with_upload_rule_and_selection_rule_client_id_string_not_equal(self):
        name = "test_16_cl_with_upload_rules"
        records_count = 49
        rule = self.files_utils.generate_rule_list(fields=["clientid"], types=["string"],
                                                   operators=["not equal"], values=["Higgins"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_17_post_list_with_upload_rule_and_selection_rule_first_name_and_company_equal(self):
        name = "test_17_cl_with_upload_rules"
        records_count = 10
        rule = self.files_utils.generate_rule_list(fields=["firstname", "companyname"], types=["string", "string"],
                                                   operators=["equal", "equal"], values=["Alex", "Miller Ltd"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_18_post_list_with_upload_rule_and_selection_rule_first_name_and_company_not_equal(self):
        name = "test_18_cl_with_upload_rules"
        records_count = 40
        rule = self.files_utils.generate_rule_list(fields=["firstname", "companyname"], types=["string", "string"],
                                                   operators=["not equal", "not equal"], values=["Alex", "Miller Ltd"])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection', rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_19_post_list_with_upload_rule_and_selection_rule_first_name_or_company_equal(self):
        name = "test_19_cl_with_upload_rules"
        records_count = 20
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[self.f_name_equal, self.company_equal])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_20_post_list_with_upload_rule_and_selection_rule_first_name_or_company_not_equal(self):
        name = "test_20_cl_with_upload_rules"
        records_count = 50
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[self.f_name_not_equal, self.company_not_equal])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_21_post_list_with_splitting_rule_splitByCustom_1_selection_rule(self):
        name = "test_21_cl_with_upload_rules"
        records_count = 10
        rule_id = self.lists_utils.post_upload_rule(splitting_used=True, rule_type='splitByCustom',
                                                    spec_id=self.spec_id, name=name + '_rule', output_name=name + '_%d',
                                                    selection_id=self.selection_f_name_id, spec_file_name=self.spc_name)
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     split_custome=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name + "_1", object_type='CFGCallingList',
                                                          value=name + "_1", object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=str(records_count))

    def test_22_post_list_with_splitting_rule_splitByCustom_2_selection_rule(self):
        name = "test_22_cl_with_upload_rules"
        records_count = 10
        rule_id = self.lists_utils.post_upload_rule(splitting_used=True, rule_type='splitByCustom',
                                                    spec_id=self.spec_id, name=name + '_rule',
                                                    spec_file_name=self.spc_name, output_name=name + '_%d',
                                                    selection_id=[self.selection_f_name_id, self.selection_l_name_id])
        job_id = self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                              split_custome=True, check_in_db=False,
                                              mappingfile=basename(self.spec_file), rule=rule_id)
        self.lb_utils.check_result_of_list_splitting(job_id=job_id, lists_count=2, expected_list_size=records_count)

    def test_23_post_list_with_upload_rule_and_remainder(self):
        name = "test_23_cl_with_upload_rules"
        rule_id = self.lists_utils.post_upload_rule(splitting_used=True, rule_type='splitByCustom', name=name + '_rule',
                                                    spec_id=self.spec_id, output_name=name + '_%d', remainder=True,
                                                    selection_id=self.selection_f_name_id, spec_file_name=self.spc_name)
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name, check_in_db=False,
                                     split_custome=True, mappingfile=basename(self.spec_file), rule=rule_id)
        normal_list = self.conf_server.check_in_cme_by_name(name=name + '_1', object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=normal_list, parameter='size', value='10')
        remainder_list = self.conf_server.check_in_cme_by_name(name=name + '_1_remainder', object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=remainder_list, parameter='size', value='40')

    def test_24_post_list_with_upload_rules_to_process_records_per_waterfall_rule(self):
        name = "test_24_cl_with_upload_rules"
        records_count = sorted([10, 10, 30])
        divided_lists_size = []
        all_records = []
        rule_list = self.files_utils.generate_rule_list(fields=["other5"], types=["string"], operators=["is empty"])
        selection_id = self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_other5',
                                                                         rules=[rule_list])
        rule_id = self.lists_utils.post_upload_rule(splitting_used=True, rule_type='splitByCustom',
                                                    spec_id=self.spec_id, name=name + '_rule',
                                                    spec_file_name=self.spc_name, output_name=name + '_%d',
                                                    selection_id=[self.selection_f_name_id, self.selection_l_name_id,
                                                                  selection_id])
        job_id = self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                              split_custome=True, check_in_db=False,
                                              mappingfile=basename(self.spec_file), rule=rule_id)
        lists = self.api.get_job_from_redis(job_id)["attributes"]["Lists"]
        assert len(lists) == 3, "List divided incorrectly, expected number of lists - 3, actual {}".format(len(lists))
        for item in lists:
            list_id = self.lists_utils.get_list_id('list', item)
            divided_list_size = int(json.loads(self.lists_utils.get_lists('list', lists_id=list_id).content)
                                    ["data"]["attributes"]["size"])
            divided_lists_size.append(divided_list_size)
            records = set(self.db_utils.get_records_from_db_with_parameters(table_name='cc_list_' + str(list_id),
                                                                            column_names='c_client_id'))
            all_records.append(records)
        count = len(all_records[0]) + len(all_records[1]) + len(all_records[2])
        assert count == 50, "Not all record present in db. Expected  count of records: 50. " \
                            "Actual: {} in db.".format(count)
        assert sorted(divided_lists_size) == records_count, \
            "Incorrect size of lists in annex. Expected: {0};. Actual: {1}".format(records_count,
                                                                                   sorted(divided_lists_size))

    def test_25_post_list_with_not_existing_upload_rule(self):
        name = "test_25_cl_with_upload_rules"
        rules = {"uploadRule": {"name": 'wrong', "id": '1', "splitting": 'true'}, "listid": 0}
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file),  return_result=True,
                                     listid=list_id, selection_rule=True, check_in_db=False, expected_code=404,
                                     mappingfile=basename(self.spec_file), upload_rule=rules, name=name)

    @data(*get_csv_data('LB_tests_upload_rules.csv'))
    @unpack
    def test_26_post_list_with_selection_rule_one_condition_set(self, fields, types, operators, values, size):
        """
        Test1: Post list with upload rule and selection rule Other7 numeric equal to 55555 exp size 3
        Test2: Post list with upload rule and selection rule Other7 numeric not equal to 55555 exp size 47
        Test3: Post list with upload rule and selection rule Other7 numeric less than 55555 exp size 3
        Test4: Post list with upload rule and selection rule Other7 numeric less than or equal 333 exp size 3
        Test5: Post list with upload rule and selection rule Other7 numeric greater than 55555 exp size 3
        Test6: Post list with upload rule and selection rule Other7 numeric greater than or equal 55555 exp size 6
        Test7: Post list with upload rule and selection rule Other7 numeric Custom JS Expr "${value} > 333" exp size 6
        Test8: Post list with upload rule and selection rule Other7 string equal "List Builder" exp size 5
        Test9: Post list with upload rule and selection rule Other7 string not equal "List Builder" exp size 45
        Test10: Post list with upload rule and selection rule Other7 string Custom JS Expr "${value} === Pyt" exp size 5
        Test11: Post list with upload rule and selection rule Other7 string contains "Builder" exp size 5
        Test12: Post list with upload rule and selection rule Other7 string dos not contains "List" exp size 45
        """
        name = 'test_26_field_{0}_type_{1}_operator_{2}'.format(fields, types, operators)
        rule = self.files_utils.generate_rule_list(fields=[fields], types=[types], operators=[operators],
                                                   values=[values])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=size)

    @data(*get_csv_data('LB_tests_upload_rules_1_condition_set_2_param.csv'))
    @unpack
    def test_27_post_list_with_selection_rule_one_condition_set_two_parameter(self, field1, field2, type1, type2,
                                                                              operator1, operator2, value1, value2,
                                                                              size):
        """
        Test1: Post list with upload rule and selection rule Other7 numeric and First Name string not equal to value
        Test2: Post list with upload rule and selection rule Other7 numeric and First Name string equal to value
        Test3:Post list with upload rule and selection rule Other7 numeric no equal and First Name string equal to value
        Test4: Post list with upload rule and selection rule Other7 numeric Custom JS Expression and Other5 is empty
        """
        name = 'test_27_expected_size_{}'.format(size)
        rule = self.files_utils.generate_rule_list(fields=[field1, field2], types=[type1, type2],
                                                   operators=[operator1, operator2], values=[value1, value2])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[rule])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=size)

    def test_28_post_list_with_upload_rule_only_spc_and_use_Custom_tz(self):
        """
        Create new custom time zone-> create new upload rule with specification -> create empty list -> send post
        submit job with upload rule and use custom tz = True -> check in db custom tz add to correct number
        """
        name = 'test_28_list_with_upload_rule_and_use_custom_tz'
        self.compliance_utils.post_custom_time_zones(area_code="818", country_code="1", time_zone="America/Whitehorse")
        list_file = 'list_builder/files/Calling_list_to_custom_time_zone.txt'
        self.files_utils.copy_file_to_container('lists', list_file)
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), tz=True, rule=rule_id,
                                     mappingfile=basename(self.spec_file), listid=list_id, name=name)
        self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.compliance_utils.check_time_zone_in_db_to_colling_list(list_id, "America/Whitehorse")

    @data(*get_csv_data('LB_tests_upload_rules_2_condition_set_1_param.csv'))
    @unpack
    def test_29_post_list_with_selection_rule_two_condition_set_one_parameter(self, field1, field2, type1, type2,
                                                                              operator1, operator2, value1, value2,
                                                                              size):
        """
        Test1: Post list with upload rule and selection rule Other4 numeric or First Name string equal to value
        test2:Post list with upload rule and selection rule Other4 numeric not equal or First Name string equal to value
        Test3: Post list with upload rule and selection rule Other7 ort Company Custom JS Expression
        """
        name = 'test_29_expected_size_{}'.format(size)
        rule1 = self.files_utils.generate_rule_list(fields=[field1], types=[type1], operators=[operator1],
                                                    values=[value1])
        rule2 = self.files_utils.generate_rule_list(fields=[field2], types=[type2], operators=[operator2],
                                                    values=[value2])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[rule1, rule2])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=size)

    @data(*get_csv_data('LB_tests_upload_rules_first_condition_with_2_param_second_with_1.csv'))
    @unpack
    def test_30_post_list_with_selection_rule_first_condition_with_2_param_second_with_1(
            self, field1, type1, operator1, value1, field2, type2, operator2, value2, field3, type3, operator3, value3,
            size):
        """
        Posts lists with upload rule and selection rule. All selection rule have 2 condition set: in first condition set
        have two parameters (result after first condition set must satisfy all parameters), and second condition set
        have one parameter.
        """
        name = 'test_30_expected_size_{}'.format(size)
        rule1 = self.files_utils.generate_rule_list(fields=[field1, field2], types=[type1, type2],
                                                    operators=[operator1, operator2], values=[value1, value2])
        rule2 = self.files_utils.generate_rule_list(fields=[field3], types=[type3], operators=[operator3],
                                                    values=[value3])
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                          rules=[rule1, rule2])
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id,
                                                    spec_file_name=self.spc_name, selection_name=name + '_selection')
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name,
                                     listid=list_id, selection_rule=True, check_in_db=False,
                                     mappingfile=basename(self.spec_file), rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList', value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=size)

    def test_31_post_list_with_upload_rule_splitByQuantity_and_remainder(self):
        name = "test_31_cl_with_upload_rules"
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, value='24',
                                                    spec_file_name=self.spc_name, splitting_used=True, remainder=True,
                                                    rule_type='splitByQuantity', output_name=name + '_%d')
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name, check_in_db=False,
                                     split_custome=True, mappingfile=basename(self.spec_file), rule=rule_id)
        remainder_list = self.conf_server.check_in_cme_by_name(name=name + '_3_remainder', object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=remainder_list, parameter='size', value='2')

    def test_32_post_list_with_upload_rule_splitByPercent_and_remainder(self):
        name = "test_32_cl_with_upload_rules"
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, value='33',
                                                    spec_file_name=self.spc_name, splitting_used=True, remainder=True,
                                                    rule_type='splitByPercent', output_name=name + '_%d')
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name, check_in_db=False,
                                     split_custome=True, mappingfile=basename(self.spec_file), rule=rule_id)
        remainder_list = self.conf_server.check_in_cme_by_name(name=name + '_4_remainder', object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=remainder_list, parameter='size', value='2')

    def test_33_post_list_with_same_rule_and_check_splitting_name(self):
        name = "test_33"
        records_count = 50
        quantity = records_count / 2
        list_file = self.files_utils.make_file("calling list", name=name, records_count=records_count)
        self.files_utils.copy_file_to_container('lists', list_file)
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, value='50',
                                                    spec_file_name=self.spc_name, splitting_used=True,
                                                    rule_type='splitByPercent', output_name=name + '_%d')
        job_id = self.lb_utils.post_submitjob(importfile=basename(list_file),
                                              mappingfile=basename(self.spec_file), name=name,
                                              rule=rule_id, check_in_db=False)
        self.lb_utils.check_result_of_list_splitting(job_id=job_id, lists_count=2, expected_list_size=quantity)
        job = self.lb_utils.post_submitjob(importfile=basename(self.list_file),
                                           mappingfile=basename(self.spec_file), name=name,
                                           rule=rule_id, check_in_db=False)
        lists = self.lb_utils.api.get_job_from_redis(job)["attributes"]["Lists"]
        for i in lists:
            divided_name = i.split('_')
            regexp_1 = compile("[0-9]{7,8}$")
            regexp_2 = compile("[0-9]{6}$")
            if not regexp_1.match(divided_name[3]) and not regexp_2.match(divided_name[4]):
                raise Exception("Name of list is incorrect: {0}.".format(i))

    def tests_34_post_list_with_upload_rule_splitByField_and_remainder(self):
        name = "test_34_cl_with_upload_rules"
        self.settings_utils.put_settings(payload=self.payload_settings, set_id=self.set_id, max_lists_split=3)
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, field_name="Client ID",
                                                    spec_file_name=self.spc_name, splitting_used=True, remainder=True,
                                                    rule_type='splitByField', output_name=name + '_%d', waterfall=False)
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name, check_in_db=False,
                                     split_custome=True, mappingfile=basename(self.spec_file), rule=rule_id)
        remainder_list = self.conf_server.check_in_cme_by_name(name=name + '_4_remainder', object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=remainder_list, parameter='size', value=47)

    @data(("Device1", "area code", "equal"), ("Device2", "area code", "not equal"),
          ("Device3", "exchange", "equal"), ("Device4", "exchange", "not equal"),
          ("Device1", "exchange", "in"), ("Device2", "area code", "in"),
          ("Device3", "exchange", "is empty"), ("Device4", "area code", "is empty"))
    @unpack
    def test_35_post_call_list_with_upload_rule_device_types(self, field, types, op):
        name = "test_35_cl_with_rule_{0}_{2}_{1}".format(field, str(types).replace(" ", ''), str(op).replace(" ", ''))
        f = self.files_utils.get_file_info_for_rules(upload_file=self.cl_for_rules, rule_field=field, rule_op=op,
                                                     rule_type=types)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]])
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name, waterfall=True)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_rules), name=name, listid=list_id,
                                     selection_rule=True, check_in_db=False, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGCallingList", value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    @data(("Device1", "country code", "equal"), ("Device2", "country code", "not equal"),
          ("Device3", "timezone", "equal"), ("Device4", "timezone", "not equal"),
          ("Device3", "state code", "equal"), ("Device4", "state code", "not equal"),
          ("Device1", "country code", "in"), ("Device4", "timezone", "in"), ("Device3", "state code", "in"),
          ("Device2", "country code", "is empty"), ("Device4", "state code", "is empty"),
          ("Device3", "timezone", "is empty"))
    @unpack
    def test_36_post_call_list_with_upload_rule_device_types(self, field, types, op):
        name = "test_36_cl_with_rule_{0}_{2}_{1}".format(field, str(types).replace(" ", ''), str(op).replace(" ", ''))
        column = "cd_country_code_iso" if types == "country code" else "cd_state_code" if types == "state code" \
            else "cd_tz_name"
        db_rec = self.db_utils.get_records_from_db_with_parameters(
            table_name='cc_list_{0}'.format(self.cl_for_rules2_id), column_names=column,
            parameters_and_values={'chain_n': field[6:]})
        f = self.files_utils.get_file_info_for_rules(upload_file=self.cl_for_rules2, rule_field=field, rule_op=op,
                                                     rule_type=types, db_rec=db_rec,
                                                     rule_value=("America/New_York" if types == "timezone" else None))
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]])
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name, waterfall=True)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_rules2), name=name, listid=list_id,
                                     selection_rule=True, check_in_db=False, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGCallingList")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    @data(("First Name", "equal", "APPEND_AND_UPDATE"), ("Last Name", "not equal", "APPEND_ONLY"),
          ("Company", "not equal", "FLUSH_APPEND"))
    @unpack
    def test_37_update_call_list_with_rules(self, field, op, upload_mode):
        name = "test_37_update_call_list_with_rules_{}".format(upload_mode)
        f = self.files_utils.get_file_info_for_rules(
            upload_file=self.cl_for_rules, rule_field=field, rule_op=op, rule_type="string", upload_mode=upload_mode,
            update_file=self.cl_for_rules2)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]])
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_rules), name=name, selection_rule=True,
                                     listid=list_id, check_in_db=False, rule=rule_id)
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_rules2), name=name, check_in_db=False,
                                     selection_rule=True, rule=rule_id, listid=list_id, uploadmode=upload_mode)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGCallingList")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    @data(("Device1", "area code", "equal", "APPEND_AND_UPDATE"), ("Device2", "area code", "not equal", "APPEND_ONLY"),
          ("Device3", "exchange", "equal", "FLUSH_APPEND"))
    @unpack
    def test_38_update_call_list_with_rules_with_the_same_list(self, field, rule_type, op, upload_mode):
        name = "test_38_update_call_list_with_rules_with_the_same_list_{}".format(upload_mode)
        f = self.files_utils.get_file_info_for_rules(
            upload_file=self.cl_for_rules, rule_field=field, rule_op=op, rule_type=rule_type, upload_mode=upload_mode,
            update_file=self.cl_for_rules)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]])
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_rules), name=name, selection_rule=True,
                                     listid=list_id, check_in_db=False, rule=rule_id)
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_rules), name=name, check_in_db=False,
                                     selection_rule=True, rule=rule_id, listid=list_id, uploadmode=upload_mode)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGCallingList")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    def test_39_lb_must_Add_importing_status_for_lists_with_splitting(self):
        name = "test_39_import_status"
        files = "api_aggregator/files/empty_upload_file.txt"
        zipped = self.files_utils.make_zip_or_gzip(files=files, name=basename(files), extension='zip')
        self.files_utils.copy_file_to_container('lists', zipped)
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, value='20',
                                                    spec_file_name=self.spc_name, splitting_used=True,
                                                    rule_type='splitByPercent', output_name=name + '_%d')
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), name=name, rule=rule_id,
                                              mappingfile=basename(self.spec_file), check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: Empty file in archive', 'Incorrect response received from job: {}'.format(resp)

    @data("contact_info_type=1 AND contact_info='+13513374585'", "cd_device_index=2 AND cd_area_code='213'",
          "cd_device_index=3 AND contact_info='+19709310303'", "cd_device_index=4 AND cd_exchange='928'",
          "cd_device_index=5 AND cd_state_code='NE'", "contact_info_type=10 AND contact_info='kjones@yahoo.com'")
    def test_40_export_list_with_rule_sql_query_device(self, query):
        name = "test_40_{}".format(query)
        rule_id = self.lists_utils.post_lists_rules_or_triggerrules(
            lists_type='rules', name=name + '_selection', rules=[], script_type="selection_rule_advanced",
            useVisualEditor=False, query=query)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file),
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name)
        export_file = self.lb_utils.export_list(lists_id=list_id, list_type='cl', compare=1, selection_id=rule_id)
        self.files_utils.validate_records_in_db_or_export(self.cl_for_upload_file, export_file, export=True)

    def test_41_export_list_with_rule_operator_in(self):
        name = "test_41"
        rule = [{"field": "other7", "type": "string", "operator": "in", "value": ['333', '55555']}]
        rule_id = self.lists_utils.post_lists_rules_or_triggerrules(
            lists_type='rules', name=name + '_selection', rules=[rule], script_type="selection_rule_advanced")
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file),
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name)
        export_file = self.lb_utils.export_list(lists_id=list_id, list_type='cl', compare=27, selection_id=rule_id)
        self.files_utils.validate_records_in_db_or_export(self.cl_for_upload_file, export_file, export=True)

    def test_42_export_list_with_rule_operator_is_null(self):
        name = "test_42"
        rule = [{"field": "call_time", "type": "numeric", "operator": "is null", "value": ""}]
        rule_id = self.lists_utils.post_lists_rules_or_triggerrules(
            lists_type='rules', name=name + '_selection', rules=[rule], script_type="selection_rule_advanced")
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file),
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name)
        export_file = self.lb_utils.export_list(lists_id=list_id, list_type='cl', compare=174, selection_id=rule_id)
        self.files_utils.validate_records_in_db_or_export(self.cl_for_upload_file, export_file, export=True)

    @data(("record_status", "numeric", "equal", 1, 174), ("record_id", "numeric", "equal", "2", 1),
          ("record_type", "numeric", "equal", 2, 174), ("contact_info", "string", "equal", "+12514365056", 1),
          ("c_country_code_iso", "string", "equal", "US", 174), ("attempt", "numeric", "equal", "0", 174),
          ("contact_info_type", "numeric", "equal", 10, 13))
    @unpack
    def test_43_export_list_with_advanced_rule_new_field(self, field, types, op, value, res):
        name = "test_43_{0}_{2}_{1}".format(field, types.replace(" ", ''), op.replace(" ", ''))
        rule = [{"field": field, "type": types, "operator": op, "value": value}]
        rule_id = self.lists_utils.post_lists_rules_or_triggerrules(
            lists_type='rules', name=name + '_selection', rules=[rule], script_type="selection_rule_advanced")
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file),
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name)
        export_file = self.lb_utils.export_list(lists_id=list_id, list_type='cl', compare=res, selection_id=rule_id)
        self.files_utils.validate_records_in_db_or_export(self.cl_for_upload_file, export_file, export=True)

    def test_44_lb_must_use_selection_rule_while_exporting(self):
        name = "test_44"
        rule = self.files_utils.generate_rule_list(fields=["firstname", "companyname"], types=["string", "string"],
                                                   operators=["equal", "equal"], values=["Alex", "Miller Ltd"])
        rule_id = self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name + '_selection',
                                                                    rules=[rule], script_type="selection_rule_advanced")
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file),
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name)
        export_file = self.lb_utils.export_list(lists_id=list_id, list_type='cl', compare=42, selection_id=rule_id)
        self.files_utils.validate_records_in_db_or_export(self.cl_for_upload_file, export_file, export=True)

    def test_45_lb_must_default_use_upload_rule_name_for_remainder_file_if_percent_r(self):
        name = "test_45_"
        rule_id = self.lists_utils.post_upload_rule(splitting_used=True, rule_type='splitByCustom', name=name + '_rule',
                                                    spec_id=self.spec_id, output_name=name + '%r', remainder=True,
                                                    selection_id=self.selection_f_name_id, spec_file_name=self.spc_name)
        self.lb_utils.post_submitjob(importfile=basename(self.cl_for_upload_file), name=name, check_in_db=False,
                                     split_custome=True, mappingfile=basename(self.spec_file), rule=rule_id)
        normal_list = self.conf_server.check_in_cme_by_name(name=name + self.name_f_name_equal,
                                                            object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=normal_list, parameter='size', value='10')
        remainder_list = self.conf_server.check_in_cme_by_name(name="{0}{0}_rule_remainder".format(name),
                                                               object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=remainder_list, parameter='size', value='40')

    @data(("state", "equal"), ("region", "not equal"), ("statecode", "not like"), ("state_code", "like"),
          ("state", "is empty"))
    @unpack
    def test_46_post_call_list_with_StateRegion_keywords_with_rules(self, field, op):
        name = "test_46_post_call_list_with_rules_{}_{}".format(str(field).replace(' ', ''), str(op).replace(' ', ''))
        header = "fname,lname,device1,other1,{},clientid".format(field)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=10, extension="csv")
        self.files_utils.copy_file_to_container('lists', list_file)
        f = self.files_utils.get_file_info_for_rules(upload_file=list_file, rule_field=field, rule_op=op)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]])
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), name=name, selection_rule=True,
                                     listid=list_id, check_in_db=False, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGCallingList")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])
