from api_utils.outbound_test_case import OutboundBaseTestApi
from api_utils.settings_utils import SettingsUtils
from api_utils.lb_utils import LBUtils
from api_utils.lm_utils import LMUtils
from api_utils.lists_utils import ListsUtils
from api_utils.compliance_utils import ComplianceUtils
from ddt import ddt, data, unpack
from os.path import basename, abspath


@ddt
class TestCallingLists(OutboundBaseTestApi):
    @classmethod
    def setUpClass(cls):
        super(TestCallingLists, cls).setUpClass()
        cls.conf_server = cls.api.conf_server
        cls.files_utils = cls.api.file_utils
        cls.device_utils = cls.api.file_utils.device_utils
        cls.db_utils = cls.api.db_utils
        cls.string_utils = cls.api.string_utils
        cls.settings_utils = SettingsUtils()
        cls.compliance_utils = ComplianceUtils()
        cls.lb_utils = LBUtils()
        cls.lm_utils = LMUtils()
        cls.lists_utils = ListsUtils()
        # Settings are needed to check the correctness of filling some values in dB for some tests
        set_id = cls.settings_utils.get_settings(return_id=True)
        payload_settings = cls.settings_utils.settings_payload(countryCode="US", default_err_tz="Europe/Moscow")
        cls.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        cls.list_file = cls.files_utils.make_file("calling list", name='calling_list_2', records_count=50,
                                                  use_threading=True)
        cls.file_for_append = cls.files_utils.make_file("calling list", name='LB_append_2', records_count=50,
                                                        use_threading=True)
        cls.spec_file = 'list_builder/files/list_spec.spc'
        cls.list_for_dm = "api_aggregator/files/fixed_list_for_dm.txt"
        # Copy files to container
        copy_list = [cls.list_file, cls.file_for_append, cls.list_for_dm]
        [cls.files_utils.copy_file_to_container('lists', item) for item in copy_list]
        cls.files_utils.copy_file_to_container('specs', cls.spec_file)

        cls.spc_name = 'spc_for_lb_2'
        header = "FirstName,LastName,ClientID,Device1,Other1"
        values_length = ([0, 5], [5, 4], [9, 11], [20, 15], [35, 17])
        dm_info = cls.lists_utils.generate_data_mapping(header=header, values_length=values_length,
                                                        name="fixed_dm_for_cl_lb_2", mappingType='fixed',
                                                        mappingSource=None, delimiter=None)
        cls.dm_fixed = cls.lists_utils.post_data_mapping(name="fixed_dm_for_cl_lb_2", data_mapping=dm_info,
                                                         return_response=True)
        cls.spec_id = cls.lists_utils.post_specifications(name=cls.spc_name, upload_file=cls.spec_file)

    @data(5, 25, 250)
    def test_51_check_dsv_file_import(self, param):
        name = "test_51_check_DSV_file_import_with_{0}_Other_fields".format(param)
        header = self.files_utils.set_csv_header(other_count=param)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        dsv_file = self.files_utils.make_advanced_list_file(name=name, separator="|", other_count=param,
                                                            extension="dsv", header_row=header, devices_correct=True)
        self.files_utils.copy_file_to_container('lists', dsv_file)
        self.lb_utils.post_submitjob(importfile=basename(dsv_file), listid=list_id, name=name, check_in_db=False)
        self.lb_utils.check_import_in_db(basename(dsv_file), True, list_id, others_count=param, delimiter="|")

    @data(5, 500, 9999)
    def test_52_check_import_csv_file_with_device1_only_no_spec_no_header(self, records):
        name = "test_52_check_import_CSV_file_with_Device1_only_no_spec_no_header_{0}".format(records)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row="Device1", records=records,
                                                             write_header=False, extension="csv")
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.files_utils.copy_file_to_container('lists', list_file)
        self.files_utils.csv_file_to_dict(list_file)
        unique_nums = len(list(set([self.files_utils.device_utils.normalize_device(item)
                                    for item in [line.strip().replace('"', '') for line in open(list_file, 'r')]])))
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     check_in_db=False, timeout=150)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                          value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, section="CloudContact",
                                                        parameter='size', value=unique_nums)

    def test_53_update_spec_in_list_and_check_in_cme_APPEND_ONLY(self):
        name = 'test_53_update_spec_and_check_in_cme'
        new_spec_id = self.lists_utils.post_specifications(name=name + '_spc', upload_file=self.spec_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), spc_id=self.spec_id,
                                     mappingfile=basename(self.spec_file), listid=list_id, name=name)
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList')
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                          value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='specificationId',
                                                        value=self.spec_id)
        self.lb_utils.post_submitjob(importfile=basename(self.file_for_append), spc_id=new_spec_id,
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name, uploadmode="APPEND_ONLY", check_in_db=False)
        new_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                         value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=new_info, parameter='specificationId',
                                                        value=new_spec_id)
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList', list_update=True)

    def test_54_update_spec_in_list_and_check_in_cme_FLUSH_APPEND(self):
        name = 'test_54_update_spec_and_check_in_cme'
        new_spec_id = self.lists_utils.post_specifications(name=name + '_spc', upload_file=self.spec_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), spc_id=self.spec_id,
                                     mappingfile=basename(self.spec_file), listid=list_id, name=name)
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList')
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                          value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='specificationId',
                                                        value=self.spec_id)
        self.lb_utils.post_submitjob(importfile=basename(self.file_for_append), spc_id=new_spec_id,
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name, uploadmode="FLUSH_APPEND")
        new_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                         value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=new_info, parameter='specificationId',
                                                        value=new_spec_id)
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList', list_update=True)

    def test_55_update_spec_in_list_and_check_in_cme_APPEND_AND_UPDATE(self):
        name = 'test_55_update_spec_and_check_in_cme'
        new_spec_id = self.lists_utils.post_specifications(name=name + '_spc', upload_file=self.spec_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), spc_id=self.spec_id,
                                     mappingfile=basename(self.spec_file), listid=list_id, name=name)
        before = self.db_utils.get_records_from_db_with_parameters(table_name='cc_list_' + str(list_id))
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                          value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='specificationId',
                                                        value=self.spec_id)
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList')
        self.lb_utils.post_submitjob(importfile=basename(self.file_for_append), spc_id=new_spec_id,
                                     mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name, uploadmode="APPEND_AND_UPDATE", check_in_db=False)
        self.lb_utils.check_import_in_db(basename(self.file_for_append), True, list_id, before=before)
        new_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                         value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=new_info, parameter='specificationId',
                                                        value=new_spec_id)
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList', list_update=True)

    def test_55_post_list_without_header_and_spc_fixed_length(self):
        name = 'test_55_spc_with_fixed_length'
        spc_file = 'api_aggregator/files/fixed_length.spc'
        cl_file = 'api_aggregator/files/fixed_length_file.txt'
        self.files_utils.copy_file_to_container('specs', spc_file)
        self.files_utils.copy_file_to_container('lists', cl_file)
        spc_id = self.lists_utils.post_specifications(name=name + "_spec", upload_file=abspath(spc_file))
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(cl_file), spc_id=spc_id, check_in_db=False,
                                     mappingfile=basename(spc_file), listid=list_id, name=name)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                          value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value='4')

    def test_56_post_zip_file(self):
        name = 'test_56_post_zip_file'
        files = abspath(self.list_file)
        zipped = self.files_utils.make_zip_or_gzip(files=files, name=basename(files), extension='zip')
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(zipped), mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name, check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                          value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=50)
        expected_size = self.lists_utils.device_utils.get_devices_or_client_from_list(files)
        table_name = 'cc_list_' + str(list_id)
        record = self.lists_utils.db_utils.get_records_from_db_with_parameters(table_name=table_name)
        assert len(expected_size) == len(record), "Verification in db FAILED. Expected size is {0}." \
                                                  " Actual size: {1}".format(len(expected_size), len(record))
        self.files_utils.validate_records_in_db_or_export(files, record)

    def test_57_post_gzip_file(self):
        name = 'test_57_post_zip_file'
        files = abspath(self.list_file)
        zipped = self.files_utils.make_zip_or_gzip(files=files, name=basename(files), extension='zip')
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(zipped), mappingfile=basename(self.spec_file),
                                     listid=list_id, name=name, check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList',
                                                          value=name, object_property='name')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size',
                                                        value=50)
        expected_size = self.lists_utils.device_utils.get_devices_or_client_from_list(files)
        table_name = 'cc_list_' + str(list_id)
        record = self.lists_utils.db_utils.get_records_from_db_with_parameters(table_name=table_name)
        assert len(expected_size) == len(record), "Verification in db FAILED. Expected size is {0}." \
                                                  " Actual size: {1}".format(len(expected_size), len(record))
        self.files_utils.validate_records_in_db_or_export(files, record)

    def test_58_post_wrong_extension_list_in_zip(self):
        name = "test_58_post_wrong_extension_list_in_zip"
        files = "api_aggregator/files/upload_file.html"
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(files), name=basename(files), extension='zip')
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              mappingfile=basename(self.spec_file), check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: Unsupported file extension in archive', 'Incorrect response ' \
                                                                       'received from job: {}'.format(resp)

    def test_59_post_empty_list_in_zip(self):
        name = "test_59_post_empty_list_in_zip"
        files = "api_aggregator/files/empty_upload_file.txt"
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(files), name=basename(files), extension='zip')
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              mappingfile=basename(self.spec_file), check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: Empty file in archive', 'Incorrect response received from job: {}'.format(resp)

    def test_60_post_wrong_extension_list_in_gz(self):
        name = "test_60_post_wrong_extension_list_in_gz"
        files = "api_aggregator/files/upload_file.html"
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(files), name=basename(files), extension='gz')
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              mappingfile=basename(self.spec_file), check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: Unsupported file extension in archive', 'Incorrect response ' \
                                                                       'received from job: {}'.format(resp)

    def test_61_post_empty_list_in_gz(self):
        name = "test_61_post_empty_list_in_gz"
        files = "api_aggregator/files/empty_upload_file.txt"
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(files), name=basename(files), extension='gz')
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              mappingfile=basename(self.spec_file), check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: Empty file in archive', 'Incorrect response received from job: {}'.format(resp)

    def test_62_export_cl_with_not_existing_rule(self):
        name = "test_62"
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.export_list(lists_id=list_id, list_type='cl', compare=29, selection_id='111', expected_code=500,
                                  negative_test=True)

    def test_63_export_cl_with_rule_id_letters(self):
        name = "test_63"
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.export_list(lists_id=list_id, list_type='cl', compare=29, selection_id='aaa', expected_code=500,
                                  negative_test=True)

    def test_64_post_2_file_in_zip(self):
        name = "test_64_post_2_file_in_zip"
        zipped = "list_builder/files/tests_2_file_in_archive.zip"
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name,
                                              mappingfile=basename(self.spec_file), check_in_db=False)
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'Error: ZIP archive contains more than one file', 'Incorrect response received' \
                                                                         ' from job: {}'.format(resp)

    def test_65_post_cl_simple_input_file_with_email(self):
        name = "test_65_post_cl_simple_input_file_with_email"
        files = "list_builder/files/Cl_with_email.csv"
        self.files_utils.copy_file_to_container('lists', files)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(files), listid=list_id, name=name, check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='3')

    def test_66_post_calling_list_with_fixed_data_mapping(self):
        name = "test_66_post_cl_with_fixed_data_mapping"
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_for_dm), listid=list_id, name=name,
                                     check_in_db=False, data_map=self.dm_fixed.content)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='5')

    @data('gz', 'zip')
    def test_67_post_calling_list_with_fixed_data_mapping_in_gz(self, ext):
        name = "test_67_post_cl_with_fixed_data_mapping_in_" + ext
        zipped = self.files_utils.make_zip_or_gzip(files=abspath(self.list_for_dm), name=basename(self.list_for_dm),
                                                   extension=ext)
        self.files_utils.copy_file_to_container('lists', zipped)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(zipped), listid=list_id, name=name, check_in_db=False,
                                     data_map=self.dm_fixed.content)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='5')

    def test_68_flush_append_list_with_data_mapping(self):
        name = "test_68_flush_append_list_with_data_mapping"
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_for_dm), listid=list_id, name=name,
                                     check_in_db=False, data_map=self.dm_fixed.content)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='5')
        self.lb_utils.post_submitjob(importfile=basename(self.file_for_append), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), uploadmode="FLUSH_APPEND")
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList', list_update=True)

    @data('APPEND_ONLY', 'APPEND_AND_UPDATE')
    def test_69_append_only_list_with_data_mapping(self, append):
        name = "test_69_append_only_list_with_data_mapping" + append
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_for_dm), listid=list_id, name=name,
                                     check_in_db=False, data_map=self.dm_fixed.content)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='5')
        before = self.db_utils.get_records_from_db_with_parameters(table_name='cc_list_' + str(list_id))
        self.lb_utils.post_submitjob(importfile=basename(self.file_for_append), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), uploadmode=append, check_in_db=False)
        self.lb_utils.check_import_in_db(basename(self.file_for_append), True, list_id, before=before)
        self.conf_server.check_altered_date_in_annex(name=name, object_type='CFGCallingList', list_update=True)

    @data([",", 1], [":", 2], ["\t", 3], [";", 4], ["|", 5])
    def test_70_post_calling_list_with_delimited_data_mapping_field_number(self, param):
        name = "test_70_cl_with_dm_field_number_{0}".format(param[1])
        header = "FirstName,LastName,Company,Device1,Other1,email,ClientID"
        types = "string,string,string,phone,string,email,numeric"
        dm = self.lists_utils.generate_data_mapping(header=header, name="DM_{}".format(name), types=types,
                                                    delimiter=param[0])
        dm_resp = self.lists_utils.post_data_mapping(name="DM_{}".format(name), data_mapping=dm, return_response=True)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=20,
                                                             separator=param[0], extension='txt')
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     data_map=dm_resp.content, check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='20')

    def test_71_post_calling_list_with_delimited_data_mapping_field_number_no_header(self):
        name = "test_71_cl_with_dm_field_number_no_header"
        header = "FirstName,LastName,Company,Device1,Other1,email,ClientID"
        dm = self.lists_utils.generate_data_mapping(header=header, name="DM_{}".format(name), numberOfHeaderRecords=0)
        dm_resp = self.lists_utils.post_data_mapping(name="DM_{}".format(name), data_mapping=dm, return_response=True)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, write_header=False,
                                                             records=20)
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     data_map=dm_resp.content, check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='20')

    @data([",", 1], [":", 2], ["\t", 3], [";", 4], ["|", 5])
    def test_72_post_calling_list_with_delimited_data_mapping_field_name(self, param):
        name = "test_72_cl_with_dm_field_name_{}".format(param[1])
        header = "FirstName,LastName,Company,Device1,Device2,Other1,Other2,workemail,ClientID"
        dm = self.lists_utils.generate_data_mapping(header=header, name="DM_{}".format(name), mappingSource="fieldName",
                                                    delimiter=param[0])
        dm_resp = self.lists_utils.post_data_mapping(name="DM_{}".format(name), data_mapping=dm, return_response=True)
        list_file = self.files_utils.make_advanced_list_file(name=name, header_row=header, records=20,
                                                             separator=param[0], extension='txt')
        self.files_utils.copy_file_to_container('lists', list_file)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(list_file), listid=list_id, name=name,
                                     data_map=dm_resp.content, check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='20')

    def test_73_post_calling_list_with_incorrect_data_mapping(self):
        name = "test_73_post_calling_list_with_incorrect_data_mapping"
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        resp = self.lb_utils.post_submitjob(importfile=basename(self.list_for_dm), listid=list_id, name=name,
                                            check_in_db=False, expected_code=500, return_result=True,
                                            negative_test={'mappingSchemaId': '1', 'mappingSchema': {}})
        self.string_utils.assert_message_from_response(response=resp, expected_message='Wrong Mapping Schema: null')

    def test_74_post_calling_list_with_incorrect_upload_rule(self):
        name = "test_74_post_calling_list_with_incorrect_upload_rule"
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', spec_id=self.spec_id, value='20',
                                                    spec_file_name=self.spc_name, splitting_used=True,
                                                    rule_type='splitByPercent', output_name=name + '_%d')
        resp = self.lb_utils.post_submitjob(importfile=basename(self.list_for_dm), listid=list_id, name=name,
                                            check_in_db=False, expected_code=500, return_result=True,
                                            negative_test={"uploadRule": {"name": name + '_rule', "id": str(rule_id),
                                                                          "splitting": {}}, "listid": 0})
        self.string_utils.assert_message_from_response(
            response=resp,  expected_message="Failure processing upload rule test_74_post_calling_list_with_incorrect_"
                                             "upload_rule_rule")

    @data('APPEND_ONLY', 'APPEND_AND_UPDATE')
    def test_75_add_contact_to_empty_cl(self, append):
        name = "test_75_add_contact_to_empty_cl_{}".format(append)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode=append)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='1')

    @data('APPEND_ONLY', 'APPEND_AND_UPDATE')
    def test_76_add_contact_to_cl(self, append):
        name = "test_76_add_contact_to_cl_{}".format(append)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file))
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode=append)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='51')

    def test_77_post_calling_list_with_fixed_data_mapping_in_upload_rule(self):
        name = "test_77_cl_with_upload_and_dm"
        dm_id = self.dm_fixed.json()["data"]["internalId"]
        rule_id = self.lists_utils.post_upload_rule(name=name + '_rule', mapping_name=name, data_map_id=dm_id,
                                                    spec_file_name=False)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_for_dm), listid=list_id, name=name,
                                     check_in_db=False, data_map=self.dm_fixed.content, rule=rule_id)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='5')

    def test_78_check_default_error_tz(self):
        name = "test_78_check_default_error_tz"
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        files = 'list_builder/files/cl_error_tz.csv'
        self.files_utils.copy_file_to_container('lists', files)
        self.lb_utils.post_submitjob(importfile=basename(files), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file))
        self.compliance_utils.check_time_zone_in_db_to_colling_list(db_id=list_id, time_zone="Europe/Moscow")

    def test_79_add_contact_to_not_existing_list(self):
        resp = self.lb_utils.add_contact_to_cl(lists_id=1, expected_code=404)
        self.string_utils.assert_message_from_response(response=resp, expected_message='Unable to find list ID=1')

    def test_80_add_contact_no_numeric_id(self):
        self.lb_utils.add_contact_to_cl(lists_id='a', expected_code=[400, 652])

    def test_81_post_list_with_incorrect_regex_in_spc(self):
        name = "test_81_post_list_with_incorrect_regex_in_spc"
        files = 'list_builder/files/cl_for_test_82.txt'
        spc = 'list_builder/files/spc_with_incorrect_regex.spc'
        self.files_utils.copy_file_to_container('lists', files)
        self.files_utils.copy_file_to_container('specs', spc)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        job_id = self.lb_utils.post_submitjob(importfile=basename(files), listid=list_id, name=name,
                                              mappingfile=basename(spc), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='0')
        resp = self.lb_utils.job_check(job_id=job_id).json()["data"]["error"]
        assert resp == 'SyntaxError: Invalid regular expression: /^{27}(.{016})/: Nothing to repeat', \
            'Incorrect response received from job: {}'.format(resp)

    def test_82_post_list_with_GB_numbers(self):
        name = "test_82_post_list_with_GB_numbers"
        files = 'list_builder/files/cl_for_test_83.csv'
        spc = 'list_builder/files/CXContact_List_Spec_NoZip.spc'
        self.files_utils.copy_file_to_container('lists', files)
        self.files_utils.copy_file_to_container('specs', spc)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(files), listid=list_id, name=name,
                                     mappingfile=basename(spc))
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='1275')

    def test_83_post_random_list_with_GB_numbers(self):
        name = "test_83_"
        files = self.files_utils.make_file("calling list", name='123456', records_count=50, gb_number=True,
                                           extension="txt", use_threading=True)
        self.files_utils.copy_file_to_container('lists', files)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(files), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file), check_in_db=False)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='50')

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_84_add_contact_with_OCS_entry_to_empty_cl_APPEND_ONLY(self, key, value):
        name = "test_84_add_contact_to_empty_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_ONLY",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode="APPEND_ONLY", payload=payload)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='1')

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_85_add_contact_with_OCS_entry_to_empty_cl_APPEND_AND_UPDATE(self, key, value):
        name = "test_85_add_contact_to_empty_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_AND_UPDATE",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode="APPEND_AND_UPDATE", payload=payload)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='1')

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_86_add_contact_with_OCS_entry_to_cl_APPEND_ONLY(self, key, value):
        name = "test_86_add_contact_to_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file))
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_ONLY",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode='APPEND_ONLY', payload=payload)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='51')

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_87_add_contact_with_OCS_entry_to_cl_APPEND_AND_UPDATE(self, key, value):
        name = "test_87_add_contact_to_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file))
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_AND_UPDATE",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode='APPEND_AND_UPDATE', payload=payload)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='51')

    def test_88_add_the_same_contact_to_cl(self):
        name = self._testMethodName
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        payload = {"data": {"listid": list_id, "uploadMode": 'APPEND_ONLY',
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn())}}}
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode='APPEND_ONLY', payload=payload)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='1')
        resp = self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode='APPEND_ONLY', payload=payload,
                                               expected_code=[400, 651])
        self.string_utils.assert_message_from_response(response=resp,
                                                       expected_message="APPEND_ONLY: Duplicated clientId, rejected")

    def test_89_import_cl_with_non_geo_region_number(self):
        set_id = self.settings_utils.get_settings(return_id=True)
        payload_settings = self.settings_utils.settings_payload(countryCode="US", default_region="CA-MB")
        self.settings_utils.put_settings(payload=payload_settings, set_id=set_id)
        name = self._testMethodName
        files = "list_builder/files/files_with_non_geo_records.csv"
        self.files_utils.copy_file_to_container('lists', files)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(files), listid=list_id, name=name)
        db = self.db_utils.get_records_from_db_with_parameters(table_name='cc_list_' + str(list_id))
        for i in db:
            assert i['cd_state_code'] == "CA", "Incorrect state code found in DB. Expected: CA. Actual: {}".format(
                i['cd_state_code'])

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_90_add_contact_with_OCS_entry_to_empty_cl_APPEND_ONLY_with_api_key(self, key, value):
        name = "test_90_add_contact_to_empty_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_ONLY",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        api_key = self.lb_utils.get_authorization_through_api_key()
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode="APPEND_ONLY", payload=payload, token=api_key)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='1')

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_91_add_contact_with_OCS_entry_to_empty_cl_APPEND_AND_UPDATE_with_api_key(self, key, value):
        name = "test_91_add_contact_to_empty_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_AND_UPDATE",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        api_key = self.lb_utils.get_authorization_through_api_key()
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode="APPEND_AND_UPDATE", payload=payload,
                                        token=api_key)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='1')

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_92_add_contact_with_OCS_entry_to_cl_APPEND_ONLY_with_api_key(self, key, value):
        name = "test_92_add_contact_to_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file))
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_ONLY",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        api_key = self.lb_utils.get_authorization_through_api_key()
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode='APPEND_ONLY', payload=payload, token=api_key)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='51')

    @data(("record_type", 5), ("dial_sched_time", 3), ("campaign_id", 4), ("group_id", 12), ("agent_id", "id_of_agent"))
    @unpack
    def test_93_add_contact_with_OCS_entry_to_cl_APPEND_AND_UPDATE_with_api_key(self, key, value):
        name = "test_93_add_contact_to_cl_{}".format(key)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        self.lb_utils.post_submitjob(importfile=basename(self.list_file), listid=list_id, name=name,
                                     mappingfile=basename(self.spec_file))
        payload = {"data": {"listid": list_id, "uploadMode": "APPEND_AND_UPDATE",
                            "fields": {"FirstName": str(self.string_utils.fake.first_name()),
                                       "LastName": str(self.string_utils.fake.last_name()),
                                       "Device1": self.device_utils.rand_device(can_be_none=False),
                                       "ClientID": str(self.api.string_utils.fake.ssn()),
                                       key: value}}}
        api_key = self.lb_utils.get_authorization_through_api_key()
        self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode='APPEND_AND_UPDATE', payload=payload,
                                        token=api_key)
        list_info = self.conf_server.check_in_cme_by_name(name=name, object_type='CFGCallingList')
        self.conf_server.get_object_property_from_annex(object_from_cme=list_info, parameter='size', value='51')

    @data('APPEND_ONLY', 'APPEND_AND_UPDATE')
    def test_94_add_contact_with_incorrect_api_key(self, append):
        name = "test_94_add_contact_with_incorrect_api_key{}".format(append)
        list_id = self.lm_utils.post_empty_list(name=name)["id"]
        api_key = {'Authorization': 'apiKey 123456'}
        resp = self.lb_utils.add_contact_to_cl(lists_id=list_id, uploadmode=append, token=api_key, expected_code=401)
        self.string_utils.assert_message_from_response(response=resp, expected_message="invalid_token")
