from os.path import basename, abspath
from api_utils.outbound_test_case import OutboundBaseTestApi
from api_utils.settings_utils import SettingsUtils
from api_utils.lb_utils import LBUtils
from api_utils.lm_utils import LMUtils
from api_utils.lists_utils import ListsUtils
from random import sample, randint
from ddt import ddt, data, unpack
from csv import DictReader


@ddt
class TestSuppressionLists(OutboundBaseTestApi):
    @classmethod
    def setUpClass(cls):
        super(TestSuppressionLists, cls).setUpClass()
        cls.conf_server = cls.api.conf_server
        cls.files_utils = cls.api.file_utils
        cls.device_utils = cls.api.file_utils.device_utils
        cls.string_utils = cls.api.string_utils
        cls.db_utils = cls.api.db_utils
        cls.settings_utils = SettingsUtils()
        cls.lb_utils = LBUtils()
        cls.lm_utils = LMUtils()
        cls.lists_utils = ListsUtils()
        cls.suppression_list = cls.files_utils.make_file("calling list", name='suppression_list_2', records_count=200)
        cls.sl_for_rules = cls.files_utils.make_advanced_list_file(records=100, devices_correct=True, extension="csv",
                                                                   others_length=randint(10, 200),
                                                                   name="sl_for_rules_2", numeric_ids=True)
        cls.sl_for_rules2 = cls.files_utils.make_file("calling list", name='list_for_rule_2', records_count=50,
                                                      extension='csv')
        cls.spec_file = 'list_builder/files/list_spec.spc'
        cls.list_for_dm = "api_aggregator/files/fixed_list_for_dm.txt"
        copy_list = [cls.suppression_list, cls.sl_for_rules, cls.sl_for_rules2, cls.list_for_dm]

        header = "FirstName,LastName,ClientID,Device1,Other1"
        values_length = ([0, 5], [5, 4], [9, 11], [20, 15], [35, 17])
        dm_info = cls.lists_utils.generate_data_mapping(header=header, values_length=values_length,
                                                        name="fixed_dm_for_supp_2", mappingType='fixed',
                                                        mappingSource=None, delimiter=None)
        cls.dm_fixed = cls.lists_utils.post_data_mapping(name="fixed_dm_for_supp_2", data_mapping=dm_info,
                                                         return_response=True)

        [cls.files_utils.copy_file_to_container('lists', item) for item in copy_list]
        cls.mini_header = "fname,lname,device1,ClientID"
        cls.mini_spec = cls.files_utils.make_advanced_spec_file(name="mini_spec_2", field_list=cls.mini_header)
        cls.files_utils.copy_file_to_container('specs', cls.mini_spec)
        cls.files_utils.copy_file_to_container('specs', cls.spec_file)
        cls.sl_for_rules2_id = cls.lm_utils.post_list("list", basename(cls.sl_for_rules2), name='sl_for_rules2_2')['id']

    @data(("Client ID", "equal"), ("Client ID", "not equal"), ("Client ID", "less than"),
          ("Client ID", "less than or equal"), ("Client ID", "greater than"), ("Client ID", "greater than or equal"))
    @unpack
    def test_41_post_supp_list_with_upload_rule(self, field, op):
        name = "test_41_sl_with_rule_{0}_{1}".format(str(field).replace(" ", ''), str(op).replace(" ", ''))
        f = self.files_utils.get_file_info_for_rules(upload_file=self.sl_for_rules, rule_field=field, rule_op=op,
                                                     rule_type="numeric")
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]],
                                                          script_type="selection_rule_suppression")
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name,
                                                    script_type='upload_rule_suppression')
        days = randint(1, 31)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=days, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules), listid=list_id, rule=rule_id,
                                     selection_rule=True, listtype="SUPPRESSION_CLIENT", check_in_db=False, name=name)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess", value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    # CLOUDCON-4407: Test Functional for Selection Rule support for suppression lists
    @data(("Other3", 28), ("Other5", "Answer"))
    @unpack
    def test_42_test_functional_for_selection_rule_support_for_suppression_list(self, field, value):
        name = "test_42_Test_Functional_for_Selection_Rule_support_for_suppression_list_{0}".format(field)
        # for updating "call results" in DB:
        call_results = {21: "Abandoned", 33: "Answer", 6: "Busy", 52: "Cancel Record", 24: "Consult",
                        29: "Covered", 41: "Dial Error", 51: "Do Not Call", 26: "Dropped", 0: "Ok",
                        7: "No Answer", 35: "NoDialTone", 37: "NoRingBack", 20: "Overflowed", 25: "Pickedup",
                        18: "Queue Full", 22: "Redirected", 32: "Silence", 4: "System Error", 53: "Wrong Number"}
        # calling list:
        list_file = self.files_utils.make_advanced_list_file(name=name, devices_correct=True, records=500,
                                                             extension="csv")
        self.files_utils.copy_file_to_container('lists', list_file)
        c_list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=c_list_id, name=name,
                                     mappingfile=basename(self.spec_file), timeout=100)
        # update call_result and Other5 columns in DataBase(imitation of running campaign):
        self.db_utils.update_records_in_db(table_name="cc_list_{0}".format(c_list_id), records_for_update=400,
                                           columns=["call_result", "c_other5"], dict_values=call_results)
        # export modified calling list:
        exp_list = self.lb_utils.export_list(lists_id=c_list_id, list_type="cl")
        self.files_utils.copy_file_to_container('lists', exp_list)
        f_field = "call_result" if field == "Other3" else str(field).lower()
        # search Client IDs for compare
        rec_in_file = DictReader(file(exp_list))
        idx = set([(row["clientid"]) for row in rec_in_file if row[f_field] == str(value)])
        # specification file for Suppression list:
        # Other1: record_type; Other2: record_status; Other3: call_result; Other4: attempt; Other5: Call result
        fields = {"clientid": "8", "Other1": "58", "Other2": "59", "Other3": "60", "Other4": "61", "Other5": "16"}
        spec = self.files_utils.make_advanced_spec_file(name="Test_Functional", dict_for_spec=fields)
        self.files_utils.copy_file_to_container('specs', spec)
        spec_id = self.lists_utils.post_specifications(name=name, upload_file=spec)
        # upload rule with selection rule:
        rule = [{"field": field, "type": "string", "operator": "equal", "value": value}]
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name="{0}_sel".format(name), rules=[rule],
                                                          script_type="selection_rule_suppression")
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), spec_file_name=name,
                                                    selection_name="{0}_sel".format(name), spec_id=spec_id,
                                                    script_type='upload_rule_suppression')
        # post suppression list with upload rule and modified calling list:
        s_list_id = self.lm_utils.post_empty_list(name=name, expiration_date=1, list_type='suppression-lists',
                                                  suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(exp_list), mappingfile=basename(spec),
                                     listid=s_list_id, name=name, listtype="SUPPRESSION_CLIENT", check_in_db=False,
                                     rule=rule_id, selection_rule=True, timeout=100)
        # check list size in cme
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess", value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=len(idx))
        # compare Client IDs in input list and Suppression list
        with open(self.lb_utils.export_list(s_list_id, list_type="sl")) as f:
            sl_idx = ([str(line).replace('\n', '') for line in f])
        wrong_ids = [item for item in idx if item not in sl_idx]
        assert not wrong_ids, "Not all Client ID writen to Suppression list: {0}".format(wrong_ids)

    @data('zip', 'gz')
    def test_43_post_supp_list_in_(self, ext):
        name = "test_43_supp_" + ext
        zipped = self.files_utils.make_zip_or_gzip(files=self.suppression_list,
                                                   name=basename(self.suppression_list), extension=ext)
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=5, list_type='suppression-lists',
                                                suppression_type='deviceIndex')["id"]
        self.lb_utils.post_submitjob(importfile=basename(zipped), mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name, listtype='SUPPRESSION_DEVICE', check_in_db=False)
        self.lb_utils.check_import_in_db(importfile=basename(self.suppression_list),
                                         mappingfile=self.spec_file, listid=list_id, listtype='SUPPRESSION_DEVICE')

    @data('zip', 'gz')
    def test_44_post_supp_wrong_extension_in_(self, ext):
        name = "test_44_supp_" + ext
        files = "api_aggregator/files/upload_file.html"
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(files), name=basename(files),
                                                   extension=ext)
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=7, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              listtype="SUPPRESSION_CLIENT", check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: Unsupported file extension in archive', 'Incorrect response ' \
                                                                       'received from job: {}'.format(resp)

    @data('zip', 'gz')
    def test_45_post_empty_supp_in_(self, ext):
        name = "test_45_supp_" + ext
        files = "api_aggregator/files/empty_upload_file.txt"
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(files), name=basename(files),
                                                   extension=ext)
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=8, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              listtype="SUPPRESSION_CLIENT", check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: Empty file in archive', 'Incorrect response received from job: {}'.format(resp)

    @data(("Device1", "area code", "equal"), ("Device2", "area code", "not equal"),
          ("Device3", "exchange", "equal"), ("Device4", "exchange", "not equal"),
          ("Device1", "exchange", "in"), ("Device2", "area code", "in"),
          ("Device5", "exchange", "is empty"), ("Device6", "area code", "is empty"))
    @unpack
    def test_46_post_supp_list_with_upload_rule(self, field, types, op):
        name = "test_46_sl_with_rule_{0}_{2}_{1}".format(field, str(types).replace(" ", ''), str(op).replace(" ", ''))
        f = self.files_utils.get_file_info_for_rules(upload_file=self.sl_for_rules, rule_field=field, rule_op=op,
                                                     rule_type=types)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]],
                                                          script_type="selection_rule_suppression")
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name, waterfall=True,
                                                    script_type='upload_rule_suppression')
        days = randint(1, 31)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=days, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules), listtype="SUPPRESSION_CLIENT",
                                     name=name, listid=list_id, selection_rule=True, check_in_db=False, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess", value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    @data(("Device1", "country code", "equal"), ("Device2", "country code", "not equal"),
          ("Device3", "state code", "equal"), ("Device4", "state code", "not equal"),
          ("Device4", "time zone", "equal"), ("Device3", "time zone", "not equal"),
          ("Device1", "country code", "in"), ("Device3", "state code", "in"), ("Device3", "time zone", "in"),
          ("Device2", "country code", "is empty"), ("Device4", "state code", "is empty"),
          ("Device4", "time zone", "is empty"))
    @unpack
    def test_47_post_supp_list_with_upload_rule_device_types(self, field, types, op):
        name = "test_47_sl_with_rule_{0}_{2}_{1}".format(field, str(types).replace(" ", ''), str(op).replace(" ", ''))
        column = "cd_country_code_iso" if types == "country code" else "cd_state_code" if types == "state code" \
            else "cd_tz_name"
        db_rec = self.db_utils.get_records_from_db_with_parameters(
            table_name='cc_list_{0}'.format(self.sl_for_rules2_id), column_names=column,
            parameters_and_values={'chain_n': field[6:]})
        f = self.files_utils.get_file_info_for_rules(upload_file=self.sl_for_rules2, rule_field=field, rule_op=op,
                                                     rule_type=types, db_rec=db_rec)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]],
                                                          script_type="selection_rule_suppression")
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name, waterfall=True,
                                                    script_type='upload_rule_suppression')
        days = randint(1, 31)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=days, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules2), listtype="SUPPRESSION_CLIENT",
                                     name=name, listid=list_id, selection_rule=True, check_in_db=False, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess", value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    @data(("First Name", "not equal", "APPEND_AND_UPDATE"), ("Last Name", "not equal", "APPEND_ONLY"),
          ("Company", "equal", "FLUSH_APPEND"))
    @unpack
    def test_48_update_supp_list_with_rules(self, field, op, upload_mode):
        name = "test_48_update_supp_list_with_rules_{}".format(upload_mode)
        f = self.files_utils.get_file_info_for_rules(
            upload_file=self.sl_for_rules, rule_field=field, rule_op=op, rule_type="string", upload_mode=upload_mode,
            update_file=self.sl_for_rules2)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]],
                                                          script_type="selection_rule_suppression")
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name,
                                                    script_type='upload_rule_suppression')
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=randint(1, 31),
                                                list_type='suppression-lists', suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules), name=name, selection_rule=True,
                                     listtype="SUPPRESSION_CLIENT", listid=list_id, check_in_db=False, rule=rule_id)
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules2), name=name, check_in_db=False,
                                     selection_rule=True, rule=rule_id, listtype="SUPPRESSION_CLIENT", listid=list_id,
                                     uploadmode=upload_mode)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGTableAccess', list_update=True)

    @data(("Client ID", "equal", "APPEND_AND_UPDATE"), ("Client ID", "not equal", "APPEND_ONLY"),
          ("Client ID", "less than", "FLUSH_APPEND"))
    @unpack
    def test_49_update_supp_list_with_rules_with_the_same_list(self, field, op, upload_mode):
        name = "test_49_update_supp_list_with_rules_with_the_same_list_{}".format(upload_mode)
        f = self.files_utils.get_file_info_for_rules(
            upload_file=self.sl_for_rules, rule_field=field, rule_op=op, rule_type="numeric", upload_mode=upload_mode,
            update_file=self.sl_for_rules)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]],
                                                          script_type="selection_rule_suppression")
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name,
                                                    script_type='upload_rule_suppression')
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=randint(1, 31),
                                                list_type='suppression-lists', suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules), name=name, selection_rule=True,
                                     listtype="SUPPRESSION_CLIENT", listid=list_id, check_in_db=False, rule=rule_id)
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules), name=name, check_in_db=False,
                                     selection_rule=True, rule=rule_id, listtype="SUPPRESSION_CLIENT", listid=list_id,
                                     uploadmode=upload_mode)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGTableAccess', list_update=True)

    def test_50_post_supp_list_hider_without_quotes(self):
        name = "test_50_post_supp_list_hider_without_quotes"
        spc = abspath('list_builder/files/spc_for_supp_without_quotes.spc')
        files = abspath('list_builder/files/supp_without_quotes.csv')
        self.files_utils.copy_file_to_container('specs', spc)
        self.files_utils.copy_file_to_container('lists', files)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=1, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(files), mappingfile=basename(spc),
                                     listid=list_id, name=name, listtype="SUPPRESSION_CLIENT", check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='10')

    def test_51_post_2_file_in_zip(self):
        name = "test_51_post_2_file_in_zip"
        zipped = "list_builder/files/tests_2_file_in_archive.zip"
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=8, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              listtype="SUPPRESSION_CLIENT", check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: ZIP archive contains more than one file', 'Incorrect response received' \
                                                                         ' from job: {}'.format(resp)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_52_post_supp_list_with_fixed_data_mapping_client(self, supp):
        name = "test_52_post_supp_list_with_fixed_data_mapping_client" + supp[0]
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=8, list_type='suppression-lists',
                                                suppression_type=supp[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_for_dm), listid=list_id, name=name,
                                     check_in_db=False, listtype=supp[1], data_map=self.dm_fixed.content)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='5')

    @data(['ClientID', 'SUPPRESSION_CLIENT', 'gz'], ['deviceIndex', 'SUPPRESSION_DEVICE', 'gz'],
          ['ClientID', 'SUPPRESSION_CLIENT', 'zip'], ['deviceIndex', 'SUPPRESSION_DEVICE', 'zip'])
    def test_53_post_supp_list_with_fixed_data_mapping_in_gz(self, supp):
        name = "test_53_post_supp_list_with_fixed_data_mapping_in_gz" + supp[0] + supp[2]
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(self.list_for_dm),
                                                   name=basename(self.list_for_dm), extension=supp[2])
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=8, list_type='suppression-lists',
                                                suppression_type=supp[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name, check_in_db=False,
                                     data_map=self.dm_fixed.content, listtype=supp[1])
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='5')

    @data(("from", "equal"), ("till", "equal"), ("from", "not equal"), ("till", "not equal"))
    @unpack
    def test_54_post_supp_list_with_upload_rule(self, field, op):
        name = "test_54_sl_with_rule_{0}_{1}".format(str(field).replace(" ", ''), str(op).replace(" ", ''))
        f = self.files_utils.get_file_info_for_rules(upload_file=self.sl_for_rules, rule_field=field, rule_op=op)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]],
                                                          script_type='selection_rule_suppression')
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name,
                                                    script_type='upload_rule_suppression')
        list_id = self.lm_utils.post_empty_list(name=name, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.sl_for_rules), listid=list_id, rule=rule_id,
                                     selection_rule=True, listtype="SUPPRESSION_CLIENT", check_in_db=False, name=name)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess", value=name,
                                                          object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_55_add_contact_to_supp_list(self, supp_type):
        name = "test_55_{}".format(supp_type[0])
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type[0] == 'ClientID':
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value)
        else:
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_56_add_contact_to_supp_list_in_incorrect_supp_type(self, supp_type):
        name = "test_56_{}".format(supp_type[0])
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'deviceIndex' else str(
            self.device_utils.generate_number())
        if supp_type[0] == 'deviceIndex':
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value,
                                                          expected_code=400)
        else:
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value,
                                                          expected_code=400)
        self.string_utils.assert_message_from_response(response=resp,
                                                       expected_message="Validation failed. Unsupported entry type.")

    def test_57_add_contact_to_not_existing_list_id(self):
        self.lb_utils.add_contact_to_supp_list(list_id=1, list_name="asd", client_id="123456789", expected_code=404)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_58_add_contact_to_supp_list_AppendAndUpdate(self, supp_type):
        name = "test_58_{}".format(supp_type[0])
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = sample(self.device_utils.get_devices_or_client_from_list(
            self.suppression_list, 'ClientID' if supp_type[0] == 'ClientID' else 'deviceIndex', self.spec_file,
            l_type='supp'), 1)
        if supp_type[0] == 'ClientID':
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value[0], froms=123,
                                                   until=1234, appendOnly=False)
        else:
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value[0],  froms=123,
                                                   until=1234, appendOnly=False)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    @unpack
    def tests_59_check_suppress_Australian_numbers(self, list_type, supp_type):
        name = "test_59_check_suppress_AU_numbers_{}".format(supp_type)
        set_id = self.settings_utils.get_settings(return_id=True)
        payload_settings = self.settings_utils.settings_payload(countryCode="AU")
        self.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        list_file = self.files_utils.make_advanced_list_file(locale="en_AU", name=name, records=50,
                                                             header_row=self.mini_header)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=2, list_type='suppression-lists',
                                                suppression_type=list_type)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name, country="AU",
                                     mappingfile=basename(self.mini_spec), listtype=supp_type)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=50)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    @unpack
    def tests_60_check_suppress_GB_numbers(self, list_type, supp_type):
        name = "test_60_check_suppress_GB_numbers_{}".format(supp_type)
        set_id = self.settings_utils.get_settings(return_id=True)
        payload_settings = self.settings_utils.settings_payload(countryCode="GB")
        self.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        list_file = self.files_utils.make_advanced_list_file(locale="en_GB", name=name, records=50,
                                                             header_row=self.mini_header)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=2, list_type='suppression-list',
                                                suppression_type=list_type)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name, country="GB",
                                     mappingfile=basename(self.mini_spec), listtype=supp_type)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=50)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_61_add_contact_to_supp_list_with_api_key_for_authorization(self, supp_type):
        name = "test_61_{}".format(supp_type[0])
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        api_key = self.lb_utils.get_authorization_through_api_key()
        if supp_type[0] == 'ClientID':
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value, token=api_key)
        else:
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value, token=api_key)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_62_add_contact_with_incorrect_api_key(self, supp_type):
        name = "test_62_{}".format(supp_type[0])
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        api_key = {'Authorization': 'apiKey 123456'}
        if supp_type[0] == 'ClientID':
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value,
                                                          token=api_key, expected_code=401)
            self.string_utils.assert_message_from_response(response=resp, expected_message="invalid_token")
        else:
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value, token=api_key,
                                                          expected_code=401)
            self.string_utils.assert_message_from_response(response=resp, expected_message="invalid_token")

    @data(("state", "equal", "SUPPRESSION_CLIENT"), ("region", "not equal", "SUPPRESSION_CLIENT"),
          ("statecode", "not like", "SUPPRESSION_CLIENT"), ("state_code", "like", "SUPPRESSION_CLIENT"),
          ("state", "is empty", "SUPPRESSION_CLIENT"), ("state", "is empty", "SUPPRESSION_DEVICE"),
          ("state", "equal", "SUPPRESSION_DEVICE"), ("region", "not equal", "SUPPRESSION_DEVICE"),
          ("statecode", "not like", "SUPPRESSION_DEVICE"), ("state_code", "like", "SUPPRESSION_DEVICE"))
    @unpack
    def test_63_post_supp_list_with_StateRegion_keywords_with_rules(self, field, op, type_):
        name = "test_63_post_supp_list_{}_with_rules_{}_{}".format(type_, str(field).replace(' ', ''),
                                                                   str(op).replace(' ', ''))
        header = "fname,lname,device1,other1,{},clientid".format(field)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=10, extension="csv")
        self.files_utils.copy_file_to_container('lists', list_file)
        f = self.files_utils.get_file_info_for_rules(upload_file=list_file, rule_field=field, rule_op=op)
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[f["rule"]],
                                                          script_type='selection_rule_suppression')
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name,
                                                    script_type='upload_rule_suppression')
        supp_type = "ClientID" if type_ == "SUPPRESSION_CLIENT" else "deviceIndex"
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=2, list_type='suppression-lists',
                                                suppression_type=supp_type)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), name=name, selection_rule=True, listid=list_id,
                                     listtype=type_, check_in_db=False, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=f["count"])

    @data("equal", "is empty")
    def test_64_default_region_must_apply_to_supp_list_import_with_rule(self, op):
        settings = self.settings_utils.settings_payload(countryCode="US", default_region="CA")
        self.settings_utils.put_settings(payload=settings, set_id=self.settings_utils.get_settings(return_id=True))
        name = "test_64_default_region_must_apply_to_supp_list_import_with_rule_{}".format(str(op).replace(' ', '_'))
        line = 'Device1\n"+1 628-812-9541"\n"(564) 808-5389"\n"(564) 675-3333"\n"(564) 218-4035"'
        list_file = self.files_utils.make_advanced_list_file(name=name, records=0, write_header=False, write_line=line)
        self.files_utils.copy_file_to_container('lists', list_file)
        rule = [{"field": "device1", "type": "state code", "operator": op, "value": "CA" if op == "equal" else ""}]
        self.lists_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=name, rules=[rule],
                                                          script_type="selection_rule_suppression")
        rule_id = self.lists_utils.post_upload_rule(name="{0}_upload".format(name), selection_name=name,
                                                    script_type='upload_rule_suppression')
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=3, list_type='suppression-lists',
                                                suppression_type='deviceIndex')["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listtype="SUPPRESSION_DEVICE",
                                     name=name, listid=list_id, selection_rule=True, check_in_db=False, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type="CFGTableAccess")
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=0 if op == "is empty" else 4)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_65_negative_add_the_same_contact_to_supp_list(self, supp_type):
        name = "test_65_{}".format(supp_type[0])
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type[0] == 'ClientID':
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value)
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value,
                                                          expected_code=400)
            message = "Cannot store suppression entry. APPEND_ONLY: Duplicated clientId={}, rejected".format(value)
            self.string_utils.assert_message_from_response(resp, expected_message=message)
        else:
            self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value)
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value,
                                                          expected_code=400)
            message = "Cannot store suppression entry. No device(s) except duplicates, rejected"
            self.string_utils.assert_message_from_response(resp, expected_message=message)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_66_add_contact_till_from_abc(self, supp_type):
        name = self._testMethodName
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type[0] == 'ClientID':
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value, froms='abc',
                                                          until=123, expected_code=[400, 652])
            self.string_utils.assert_message_from_response(response=resp,
                                                           expected_message=['/fields/from: should be number'])
        else:
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value, froms=12345,
                                                          until='abc', expected_code=[400, 652])
            self.string_utils.assert_message_from_response(response=resp,
                                                           expected_message=['/fields/until: should be number'])

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_67_add_contact_till_from_more_then_24_hours(self, supp_type):
        name = self._testMethodName
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type[0] == 'ClientID':
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value, froms=1,
                                                          until=900000, expected_code=[400, 652])
        else:
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value, froms=12345,
                                                          until=900000, expected_code=[400, 652])
        self.string_utils.assert_message_from_response(
            response=resp, expected_message="Invalid Till value. Must be less then or equal 24 hours")

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_68_from_more_then_till_in_file(self, supp_type):
        name = self._testMethodName
        files = 'list_builder/files/calling_list_1.csv'
        self.files_utils.copy_file_to_container('lists', files)
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(files), listid=list_id, name=name,  check_in_db=False,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        rejected = self.lists_utils.get_suppression_lists(supp_list_id=list_id, download=True,
                                                          index='messages').content
        assert rejected == 'Invalid From/Till values', "Not all record return. Returned records: '{}'".format(rejected)

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_69_add_contact_from_more_then_till(self, supp_type):
        name = self._testMethodName
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type[0] == 'ClientID':
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value, froms=20,
                                                          until=5, expected_code=[400, 652])
        else:
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value, froms=20,
                                                          until=5, expected_code=[400, 652])
        self.string_utils.assert_message_from_response(
            response=resp, expected_message="Invalid From/Till values. From cannot be greater then Till")

    @data(['ClientID', 'SUPPRESSION_CLIENT'], ['deviceIndex', 'SUPPRESSION_DEVICE'])
    def test_70_add_contact_till_from_negative(self, supp_type):
        name = self._testMethodName
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type=supp_type[0])["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.suppression_list), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), listtype=supp_type[1])
        value = str(self.device_utils.fake.ssn()) if supp_type[0] == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type[0] == 'ClientID':
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=value, froms=-1,
                                                          until=1234, expected_code=[400, 652])
        else:
            resp = self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=value, froms=1234,
                                                          until=-2, expected_code=[400, 652])
        self.string_utils.assert_message_from_response(response=resp,
                                                       expected_message="Invalid From/Till values. Cannot be negative")

    def test_71_find_suppression_entry_client_id(self):
        name = self._testMethodName
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type='ClientID')["id"]
        client_id = 'asd-1234'
        self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, client_id=client_id)
        self.lb_utils.find_suppression_entry(client_id=client_id)

    def test_72_find_suppression_entry_device(self):
        name = self._testMethodName
        list_id = self.lm_utils.post_empty_list(name=name, expiration_date=-1, list_type='suppression-lists',
                                                suppression_type='deviceIndex')["id"]
        device = '+12345678911'
        self.lb_utils.add_contact_to_supp_list(list_id=list_id, list_name=name, device=device)
        self.lb_utils.find_suppression_entry(device=device)

    def test_73_find_suppression_entry_without_token(self):
        self.lb_utils.find_suppression_entry(client_id='asd-1234', token=False, expected_code=401)
