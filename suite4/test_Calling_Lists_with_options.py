from api_utils.outbound_test_case import OutboundBaseTestApi
from api_utils.settings_utils import SettingsUtils
from api_utils.lb_utils import LBUtils
from api_utils.lm_utils import LMUtils
from api_utils.lists_utils import ListsUtils
from api_utils.compliance_utils import ComplianceUtils
from ddt import ddt, data
from os.path import basename


@ddt
class TestCallingListsOptions(OutboundBaseTestApi):
    @classmethod
    def setUpClass(cls):
        super(TestCallingListsOptions, cls).setUpClass()
        cls.conf_server = cls.api.conf_server
        cls.files_utils = cls.api.file_utils
        cls.settings_utils = SettingsUtils()
        cls.compliance_utils = ComplianceUtils()
        cls.lb_utils = LBUtils()
        cls.lm_utils = LMUtils()
        cls.lists_utils = ListsUtils()
        # Settings are needed to check the correctness of filling some values in dB for some tests
        set_id = cls.settings_utils.get_settings(return_id=True)
        payload_settings = cls.settings_utils.settings_payload(countryCode="US", timeZone="America/New_York",
                                                               max_lists_split=5)
        cls.settings_utils.put_settings(payload=payload_settings, set_id=set_id)

    @data([",", 1, "csv"], [".", 2, "csv"], ["\\t", 3, "csv"], ["\\n", 4, "csv"], ["\\", 5, "csv"], ["/", 6, "csv"],
          ["|", 7, "csv"], [",", 1, "dsv"], [".", 2, "dsv"], ["\\t", 3, "dsv"], ["\\n", 4, "dsv"], ["\\", 5, "dsv"],
          ["/", 6, "dsv"], ["|", 7, "dsv"])
    def test_01_check_option_csv_field_separator(self, param):
        name = "test_01_check_option_CSVFieldSeparator_{0}_{1}".format(param[1], param[2])
        records = 10
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="CSVFieldSeparator", option_value=param[0])
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name=name, extension=param[2], records=records,
                                                            separator=param[0], write_header=False,
                                                            devices_correct=True)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=records)

    @data([1, "txt"], [25, "txt"], [2, "csv"], [15, "csv"])
    def test_02_check_option_header_count(self, param):
        name = "test_02_check_option_HeaderCount_{0}".format(param[0])
        records = 30
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="HeaderCount", option_value=param[0])
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), extension=param[1],
                                                            devices_correct=True, records=records)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=31-param[0])

    @data(1, 9, 15, "string")
    def test_03_check_option_header_count_with_empty_lines_in_csv(self, param):
        name = "test_03_check_option_HeaderCount_with_empty_lines_in_csv_{0}".format(param)
        records = 20
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="HeaderCount", option_value=param)
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), records=records,
                                                            devices_correct=True, empty_lines_before=param
                                                            if param is not "string" else 10)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=21-param
                                                        if param is not "string" else records)

    @data([0, "txt"], [25, "txt"], [1, "csv"], [30, "csv"])
    def test_04_check_option_trailer_count(self, param):
        name = "test_04_check_option_TrailerCount_{0}".format(param[0])
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="TrailerCount", option_value=param[0])
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), extension=param[1],
                                                            devices_correct=True)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=50-param[0])

    @data(2, 5, 25)
    def test_05_check_option_trailer_count_with_empty_lines_in_csv(self, param):
        name = "test_05_check_option_TrailerCount_with_empty_lines_in_csv_{0}".format(param)
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="TrailerCount", option_value=param)
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name),
                                                            devices_correct=True, empty_lines_after=param-1)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=50-param)

    @data(1, 5, 50)
    def test_06_check_option_error_limit(self, param):
        name = "test_06_check_option_ErrorLimit_{0}".format(param)
        records = 10
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="ErrorLimit", option_value=param)
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), records=records,
                                                            devices_correct=True)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=True)

    @data(1, 5, 30, "string")
    def test_07_check_option_error_limit_csv_with_incorrect_records(self, param):
        name = "test_07_check_option_ErrorLimit_{0}_incorrect_records".format(param)
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="ErrorLimit", option_value=param)
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), devices_correct=True,
                                                            wrong_records=range(11, 11+param if param is not
                                                                                "string" else 21))
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=10 if param is not "string" else 40)

    @data(1, 5, 50)
    def test_08_check_option_error_limit_percent(self, param):
        name = "test_08_check_option_ErrorLimitPercent_{0}".format(param)
        records = 10
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="ErrorLimitPercent", option_value=param)
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), records=records,
                                                            devices_correct=True)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=True)

    @data(10, 25, 50, "string")
    def test_09_check_option_error_limit_percent_csv_with_incorrect_records(self, param):
        name = "test_09_check_option_ErrorLimitPercent_{0}_incorrect_records".format(param)
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="ErrorLimitPercent", option_value=param)
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), devices_correct=True,
                                                            wrong_records=range(11, 37))
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=10 if param is not "string" else 24)

    @data(18004552020, "John_3volta", "")
    def test_10_check_option_caller_id(self, param):
        name = "test_10_check_option_CallerID_{0}".format(param)
        records = 10
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(param),
                                                        option_name="CallerID", option_value=param)
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name=name, records=records, devices_correct=True)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=True)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="OCServer",
                                                        parameter='CPNDigits', value=param if param else None)

    def test_11_check_option_caller_id_with_splitByPercent_rule(self):
        name = "test_11_check_option_CallerID_with_splitByPercent"
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="CallerID", option_value="11223344556")
        self.files_utils.copy_file_to_container('specs', spec)
        opt_list = self.files_utils.make_advanced_list_file(name=name, devices_correct=True, records=40)
        self.files_utils.copy_file_to_container('lists', opt_list)
        rule_id = self.lists_utils.post_upload_rule(name="{0}_rule".format(name), spec_id=spec, value=25,
                                                    spec_file_name="{0}_spec".format(name), splitting_used=True,
                                                    rule_type='splitByPercent', output_name="{0}_%d".format(name))
        self.lb_utils.post_submitjob(importfile=basename(opt_list), name=name,
                                     mappingfile=basename(spec), check_in_db=False, rule=rule_id)
        for i in range(1, 5):
            cme_name = "{0}_{1}".format(name, i)
            self.lm_utils.get_lists(name=cme_name, check_size=True, size=10)
            list_info = self.conf_server.check_in_cme_by_name(name=cme_name, object_type='CFGCallingList')
            self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="OCServer",
                                                            parameter='CPNDigits', value='11223344556')

    def test_12_check_option_caller_id_with_splitByQuantity_rule(self):
        name = "test_12_check_option_CallerID_with_splitByQuantity"
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        option_name="CallerID", option_value="99887766554")
        self.files_utils.copy_file_to_container('specs', spec)
        opt_list = self.files_utils.make_advanced_list_file(name=name, devices_correct=True)
        self.files_utils.copy_file_to_container('lists', opt_list)
        rule_id = self.lists_utils.post_upload_rule(name="{0}_rule".format(name), spec_id=spec, value=10,
                                                    spec_file_name="{0}_spec".format(name), splitting_used=True,
                                                    rule_type='splitByQuantity', output_name="{0}_%d".format(name))
        self.lb_utils.post_submitjob(importfile=basename(opt_list), name=name,
                                     mappingfile=basename(spec), check_in_db=False, rule=rule_id)
        for i in range(1, 6):
            cme_name = "{0}_{1}".format(name, i)
            self.lm_utils.get_lists(name=cme_name, check_size=True, size=10)
            list_info = self.conf_server.check_in_cme_by_name(name=cme_name, object_type='CFGCallingList')
            self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="OCServer",
                                                            parameter='CPNDigits', value='99887766554')

    @data([26, 15, 5], [65, 39, 2], [130, 80, 1], [5, 7, 0], ["string", 80, 1])
    def test_13_check_option_fixed_width(self, param):
        # param[0] - FixedWidth value
        # param[1] - length of field_list for spec generating
        # param[2] - expected list size
        name = "test_13_check_option_FixedWidth_{0}".format(param[0])
        field_list = "Device1,Device2,Device3,Device4,Device5,Device6,Device7,Device8,Device9,Device10"[:param[1]]
        list_line = "+12147285806,+17249589191,+17127101841,+18506565073,+13517962617," \
                    "+12047939507,+17794237192,+17242103680,+13193601780,+18435051870"
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                        field_list=list(field_list.split(",")),
                                                        option_name="FixedWidth", option_value=param[0])
        self.files_utils.copy_file_to_container('specs', spec)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        opt_list = self.files_utils.make_advanced_list_file(name="{0}_list".format(name), write_line=list_line,
                                                            records=0, write_header=False)
        self.files_utils.copy_file_to_container('lists', opt_list)
        self.lb_utils.post_submitjob(importfile=basename(opt_list), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=param[2])

    @data("tz", "timezone", "time_zone", "zip", "zip_code", "zip code", "postal_code", "postal code",
          "country", "country_code", "country code", "state", "region", "state code", "statecode", "state_code")
    def test_14_check_specification_file_keywords_in_db(self, param):
        name = "test_14_check_keywords_in_spec_file_{0}".format(str(param).replace(" ", "-"))
        records = 10
        field_list = "FirstName,LastName,Company,Device1,Device2,Other1,Other2,{0},ClientID".format(param)
        key_spec = self.files_utils.make_advanced_spec_file(name="{0}_spec".format(name), field_list=field_list)
        self.files_utils.copy_file_to_container('specs', key_spec)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=field_list, records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     mappingfile=basename(key_spec), check_in_db=True)
        self.lb_utils.check_spec_keyword_in_db(list_id=list_id, keyword=param, file_path=basename(list_file))

    @data("tz", "timezone", "time_zone", "zip", "zip_code", "zip code", "postal_code", "postal code",
          "country", "country_code", "country code", "state", "region", "state code", "statecode", "state_code")
    def test_15_check_header_row_keywords_in_db_no_spec_file(self, param):
        name = "test_15_check_keywords_in_header_row_{0}".format(str(param).replace(" ", "-"))
        records = 10
        header = "FirstName,LastName,Company,Device1,Device2,Other1,Other2,{0},ClientID".format(param)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=records,
                                                             extension="csv")
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name, check_in_db=False)
        self.lb_utils.check_import_in_db(basename(list_file), True, list_id)
        self.lb_utils.check_spec_keyword_in_db(list_id=list_id, keyword=param, file_path=basename(list_file))

    @data("email", "emailaddress", "homeemail", "workemail")
    def test_16_import_basic_email_contact_list(self, keyword):
        name = "test_16_import_basic_email_contact_list_with_{}".format(keyword)
        records = 10
        header = "FirstName,LastName,Other1,Other25,{},ClientID".format(keyword)
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spc".format(name), field_list=header)
        self.files_utils.copy_file_to_container('specs', spec)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=True)
        self.lb_utils.check_spec_keyword_in_db(list_id=list_id, keyword=keyword, file_path=basename(list_file))
        self.compliance_utils.check_time_zone_in_db_to_colling_list(list_id, "America/New_York")
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    @data("email", "emailaddress", "homeemail", "workemail")
    def test_17_import_basic_email_contact_list_no_spec(self, keyword):
        name = "test_17_import_basic_email_contact_list_with_{}_no_spec".format(keyword)
        records = 10
        header = "FirstName,LastName,Other1,Other250,{},ClientID".format(keyword)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, extension="csv",
                                                             records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name)
        self.lb_utils.check_spec_keyword_in_db(list_id=list_id, keyword=keyword, file_path=basename(list_file))
        self.compliance_utils.check_time_zone_in_db_to_colling_list(list_id, "America/New_York")
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    @data("email", "emailaddress", "homeemail", "workemail")
    def test_18_import_basic_email_contact_list_with_devices(self, keyword):
        name = "test_18_import_basic_email_contact_list_with_{}".format(keyword)
        records = 10
        devices = (",".join("Device{}".format(i) for i in range(1, 11)))
        header = "FirstName,LastName,{0},Other1,{1},ClientID".format(devices, keyword)
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spc".format(name), field_list=header)
        self.files_utils.copy_file_to_container('specs', spec)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     mappingfile=basename(spec), check_in_db=True)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    def test_19_import_CL_with_all_possible_devices_and_emails_no_spec(self):
        name = "test_19_import_CL_with_all_possible_devices_and_emails_no_spec"
        records = 10
        devices = (",".join("Device{}".format(i) for i in range(1, 11)))
        ci_devices = "homePhone,workPhone,cellPhone,VacationPhone,VoiceMail"
        e_mails = "email,emailaddress,homeemail,workemail"
        header = "FirstName,LastName,{0},{1},Other10,Other50,{2},ClientID".format(devices, ci_devices, e_mails)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, devices_correct=True,
                                                             extension="csv", records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    def tests_20_check_Australian_numbers(self):
        name = "test_20_check_AU_numbers"
        records = 20
        set_id = self.settings_utils.get_settings(return_id=True)
        payload_settings = self.settings_utils.settings_payload(countryCode="AU")
        self.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        list_file = self.files_utils.make_advanced_list_file(locale="en_AU", name=name, records=records, extension="csv",
                                                             header_row="fname,lname,device1,other1,clientid,email")
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name, country="AU")
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    def tests_21_check_GreatBritain_numbers(self):
        name = "test_21_check_GB_numbers"
        records = 20
        set_id = self.settings_utils.get_settings(return_id=True)
        payload_settings = self.settings_utils.settings_payload(countryCode="GB")
        self.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        list_file = self.files_utils.make_advanced_list_file(locale="en_GB", name=name, records=records, extension="csv",
                                                             header_row='fname,lname,device1,other1,clientid,email')
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name, country="GB")
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    @data("original record", "originalrecord", "original", "original_record")
    def test_22_import_list_with_different_original_keyword_in_spc(self, keyword):
        records = 10
        set_id = self.settings_utils.get_settings(return_id=True)
        payload_settings = self.settings_utils.settings_payload(countryCode="US")
        self.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        name = self._testMethodName
        header = "FirstName,LastName,Device1,Other1,Other2,{},ClientID".format(keyword)
        spec = self.files_utils.make_advanced_spec_file(name="{0}_spc".format(name), field_list=header)
        self.files_utils.copy_file_to_container('specs', spec)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     mappingfile=basename(spec))
        self.lb_utils.check_spec_keyword_in_db(list_id=list_id, keyword=keyword, file_path=basename(list_file))
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    @data("original record", "originalrecord", "original", "original_record")
    def test_23_import_list_with_different_original_keyword_no_spc(self, keyword):
        records = 10
        set_id = self.settings_utils.get_settings(return_id=True)
        payload_settings = self.settings_utils.settings_payload(countryCode="US")
        self.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        name = self._testMethodName
        header = "FirstName,LastName,Device1,Other1,Other2,{},ClientID".format(keyword)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, extension="csv",
                                                             records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name)
        self.lb_utils.check_spec_keyword_in_db(list_id=list_id, keyword=keyword, file_path=basename(list_file))
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)

    @data("GB", "AU", "US")
    def tests_24_check_cd_mask_international(self, country):
        name = "test_24_check_{}_cd_mask_international".format(country)
        records = 10
        settings = self.settings_utils.settings_payload(countryCode=country)
        self.settings_utils.put_settings(payload=settings, set_id=self.settings_utils.get_settings(return_id=True))
        list_file = self.files_utils.make_advanced_list_file(locale="en_{}".format(country), name=name, extension="csv",
                                                             header_row="fname,lname,device1,other1,clientid",
                                                             records=records)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name, country=country)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value=records)
        self.lb_utils.check_cd_mask(self.lb_utils.export_list(lists_id=list_id, list_type="cl"))
