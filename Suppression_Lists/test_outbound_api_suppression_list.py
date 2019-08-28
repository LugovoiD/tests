from os.path import abspath
from api_utils.outbound_test_case import OutboundBaseTestApi
from api_utils.utils import expected_failure
from api_utils.lists_utils import ListsUtils
from ddt import ddt, data
from time import sleep
from api_utils.files_utils import FilesUtils
from api_utils.utils import StringUtils
from api_utils.device_utils import DeviceUtils
from random import sample


@ddt
class TestOutboundAPISuppressionList(OutboundBaseTestApi):
    @classmethod
    def setUpClass(cls):
        super(TestOutboundAPISuppressionList, cls).setUpClass()
        cls.conf_server = cls.api.conf_server
        cls.files_utils = FilesUtils()
        cls.string_utils = StringUtils()
        cls.device_utils = DeviceUtils()
        cls.lists_utils = ListsUtils()
        cls.spec_file = abspath('api_aggregator/files/list_spec.spc')
        cls.import_activity = abspath('api_aggregator/files/Cl_for_import_activity.txt')
        cls.supp_file = cls.files_utils.make_file("calling list", name="cl_for_supp")
        cls.big_file = cls.files_utils.make_file("calling list", records_count=2000, name="big_file_for_supp")
        cls.supp_incorrect_value = cls.files_utils.make_file("suppression", extension='txt', incorrect=3,
                                                             supp_list_type='deviceIndex', name="sup_incorrect_value")
        cls.spec_id = cls.lists_utils.post_specifications(name="new_spec", upload_file=cls.spec_file)
        cls.file_for_append = cls.files_utils.make_file("calling list", records_count=20, name="cl_supp_for_append")

    def test_01_get_suppression_lists(self):
        self.lists_utils.get_suppression_lists()

    def test_02_get_not_existing_list(self):
        self.lists_utils.get_suppression_lists('1', expected_code=404)

    def test_03_get_supp_list_by_id(self):
        list_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='suppression list',
                                                         suppression_type='ClientID', expiration_date='3',
                                                         spec_id=self.spec_id, return_id=True)
        self.lists_utils.get_suppression_lists(supp_list_id=list_id)

    @data("ClientID", "deviceIndex")
    def test_04_download_supp_list(self, supp_type):
        supp_list_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file,
                                                              name='test_04_download_supp_list_device' + supp_type,
                                                              suppression_type=supp_type, expiration_date='3',
                                                              spec_id=self.spec_id, return_id=True)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_04_download_supp_list_device' + supp_type,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value=supp_type)
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value="3.0.0.0")
        size = len(self.device_utils.get_devices_or_client_from_list(self.supp_file, list_type=supp_type, l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        records_in_db = self.lists_utils.get_suppression_lists(supp_list_id=supp_list_id, download=True)
        file_to_compare = self.device_utils.get_devices_or_client_from_list(list_file=self.supp_file,
                                                                            list_type=supp_type, l_type="sup")
        self.lists_utils.compare_initial_file_with_records_in_db(file=file_to_compare, records_in_db=records_in_db,
                                                                 records_type=supp_type)

    def test_05_download_not_existing_supp_list(self):
        self.lists_utils.get_suppression_lists(supp_list_id='1', download=True, expected_code=404)

    @data("60", "1", "90", "0", "24")
    def test_06_post_supp_list_device(self, expires_value):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date=expires_value,
                                               name='test_06_post_supp_list_device' + expires_value,
                                               suppression_type='deviceIndex', spec_id=self.spec_id)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_06_post_supp_list_device' + expires_value,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value='deviceIndex')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value=expires_value + ".0.0.0")
        size = len(self.device_utils.get_devices_or_client_from_list(self.supp_file, list_type='deviceIndex',
                                                                     l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_06_post_supp_list_device' + expires_value,
                                                 parameter='suppressionField', value='deviceIndex', lists=lists)

    @data("60", "24", "1", "90", "0")
    def test_07_post_supp_list_client_id(self, expires_value):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                               name='test_07_post_supp_list_client_id' + expires_value,
                                               suppression_type='ClientID', expiration_date=expires_value)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_07_post_supp_list_client_id' + expires_value,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value='ClientID')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value=expires_value + ".0.0.0")
        size = len(self.device_utils.get_devices_or_client_from_list(self.supp_file, list_type='ClientID'))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_07_post_supp_list_client_id' + expires_value,
                                                 parameter='suppressionField', value='ClientID', lists=lists)

    @data("60", "24", "1", "90", "0")
    def test_08_post_supp_list_required(self, expires_value):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date=expires_value,
                                               name='test_08_post_supp_list_required' + expires_value,
                                               suppression_type='deviceIndex', required='true', spec_id=self.spec_id)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_08_post_supp_list_required' + expires_value,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value='deviceIndex')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value=expires_value + ".0.0.0")
        size = len(self.device_utils.get_devices_or_client_from_list(self.supp_file, list_type='deviceIndex',
                                                                     l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='required', value='true')
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_08_post_supp_list_required' + expires_value,
                                                 parameter='suppressionMandatory', value='true', lists=lists)

    @data("deviceIndex", "ClientID")
    def test_09_post_supp_list_not_required(self, supp_type):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, required='false', expiration_date='3',
                                               name='test_09_post_supp_list_not_required' + supp_type,
                                               suppression_type=supp_type, spec_id=self.spec_id)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_09_post_supp_list_not_required' + supp_type,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value=supp_type)
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value="3.0.0.0")
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='required',
                                                        value='false')
        size = len(self.device_utils.get_devices_or_client_from_list(self.supp_file, list_type=supp_type, l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_09_post_supp_list_not_required' + supp_type,
                                                 parameter='suppressionMandatory', value='false', lists=lists)

    @data("deviceIndex", "ClientID")
    def test_10_post_supp_list_with_csv_extension_no_spec(self, supp_type):
        file_csv = self.files_utils.make_file("suppression", extension='csv', supp_list_type=supp_type)
        self.lists_utils.post_suppression_list(upload_file=file_csv,
                                               name='test_10_post_supp_list_with_csv_extension' + supp_type,
                                               suppression_type=supp_type, required='true', expiration_date='3',
                                               till_from=False)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_10_post_supp_list_with_csv_extension' + supp_type,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value=supp_type)
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value="3.0.0.0")
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='required',
                                                        value='true')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value="100")
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_10_post_supp_list_with_csv_extension' + supp_type,
                                                 lists=lists)

    # @data("deviceIndex", "ClientID")
    # def test_11_post_supp_list_with_xls_extension_no_spec(self, supp_type):
    #     file_xls = self.files_utils.make_file("suppression", extension="xls", supp_list_type=supp_type)
    #     self.lists_utils.post_suppression_list(upload_file=file_xls,
    #                                            name='test_11_post_supp_list_with_xls_extension' + supp_type,
    #                                            suppression_type=supp_type, required='true', expiration_date='3')
    #     supp_list = self.conf_server.check_in_cme_by_name(name='test_11_post_supp_list_with_xls_extension'+supp_type,
    #                                                       object_type='CFGTableAccess')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
    #                                                     value=supp_type)
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
    #                                                     value='3.0.0.0')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='required',
    #                                                     value='true')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value="100")
    #     lists = self.lists_utils.get_suppression_lists()
    #     self.lists_utils.get_list_with_parameter(name='test_11_post_supp_list_with_xls_extension' + supp_type,
    #                                              lists=lists)
    #
    # @data("deviceIndex", "ClientID")
    # def test_12_post_supp_list_with_xlsx_extension_no_spec(self, supp_type):
    #     xlsx = self.files_utils.make_file("suppression", extension="csv", supp_list_type=supp_type)
    #     self.lists_utils.post_suppression_list(upload_file=xlsx,
    #                                            name='test_12_post_supp_list_with_xlsx_extension' + supp_type,
    #                                            suppression_type=supp_type, required='true', expiration_date='3')
    #     supp_list = self.conf_server.check_in_cme_by_name(name='test_12_post_supp_list_with_xlsx_extension'+supp_type,
    #                                                       object_type='CFGTableAccess')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
    #                                                     value=supp_type)
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
    #                                                     value='3.0.0.0')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='required',
    #                                                     value='true')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value="100")
    #     lists = self.lists_utils.get_suppression_lists()
    #     self.lists_utils.get_list_with_parameter(name='test_12_post_supp_list_with_xlsx_extension' + supp_type,
    #                                              lists=lists)

    @data("deviceIndex", "ClientID")
    def test_13_post_supp_file_invalid_extension(self, supp_type):
        supp_file = abspath('api_aggregator/files/upload_file.html')
        self.lists_utils.post_suppression_list(upload_file=supp_file, expiration_date='5', check_in_db=False,
                                               name='test_13_post_supp_file_invalid_extension' + supp_type,
                                               suppression_type=supp_type, expected_code=400, spec_id=self.spec_id)
        self.conf_server.check_in_cme_by_name(name='test_13_post_supp_file_invalid_extension' + supp_type,
                                              object_type='CFGTableAccess', should_exist=False)
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_13_post_supp_file_invalid_extension' + supp_type,
                                                 lists=lists, exists=False)

    @data("deviceIndex", "ClientID")
    def test_14_post_supp_file_with_empty_file(self, supp_type):
        supp_file = abspath('api_aggregator/files/empty_upload_file.txt')
        self.lists_utils.post_suppression_list(name='test_14_post_supp_file_with_empty_file' + supp_type,
                                               upload_file=supp_file, suppression_type=supp_type, expiration_date='5',
                                               check_in_db=False, expected_code=400, spec_id=self.spec_id)

    def test_15_post_supp_file_without_name(self):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='', suppression_type='deviceIndex',
                                               expiration_date='5', expected_code=400, check_in_db=False,
                                               spec_id=self.spec_id)

    def test_16_post_existing_supp_list(self):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='test_16_post_existing_supp_list',
                                               suppression_type='deviceIndex', expiration_date='3',
                                               spec_id=self.spec_id)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_16_post_existing_supp_list',
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value='deviceIndex')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value='3.0.0.0')
        size = len(self.device_utils.get_devices_or_client_from_list(self.supp_file, list_type='deviceIndex',
                                                                     l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='test_16_post_existing_supp_list',
                                               suppression_type='deviceIndex', expiration_date='3', check_in_db=False,
                                               expected_code=409, spec_id=self.spec_id)

    def test_17_delete_supp_list(self):
        supp_list = self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='test_17_delete_supp_list',
                                                           suppression_type='deviceIndex', expiration_date='3',
                                                           spec_id=self.spec_id, return_id=True)
        self.conf_server.check_in_cme_by_name(name='test_17_delete_supp_list', object_type='CFGTableAccess')
        self.lists_utils.delete_suppression_list(supp_list_id=supp_list)
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_17_delete_supp_list', lists=lists, exists=False)

    def test_18_delete_not_existing_list(self):
        self.lists_utils.delete_suppression_list(supp_list_id='1', expected_code=404)

    def test_19_get_suppression_lists_no_auth(self):
        self.lists_utils.get_suppression_lists(auth=False, expected_code=401)

    def test_20_get_not_existing_list_no_auth(self):
        self.lists_utils.get_suppression_lists('1', auth=False, expected_code=401)

    def test_21_get_supp_list_by_id_no_auth(self):
        self.lists_utils.get_suppression_lists('1234', auth=False, expected_code=401)

    def test_22_delete_supp_list_by_id_no_auth(self):
        self.lists_utils.delete_suppression_list('1234', auth=False, expected_code=401)

    def test_23_post_supp_list_no_auth(self):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='test_23_post_supp_list_no_auth',
                                               suppression_type='deviceIndex', expiration_date='3', check_in_db=False,
                                               auth=False, expected_code=401, spec_id=self.spec_id)

    @data("deviceIndex", "ClientID")
    def test_24_post_supp_list_with_spec(self, supp_type):
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date='3',
                                               name='test_24_post_supp_list_with_spec_file' + supp_type,
                                               suppression_type=supp_type, spec_id=self.spec_id)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_24_post_supp_list_with_spec_file' + supp_type,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value=supp_type)
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value='3.0.0.0')
        size = len(self.device_utils.get_devices_or_client_from_list(self.supp_file, list_type=supp_type, l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_24_post_supp_list_with_spec_file' + supp_type,
                                                 parameter='suppressionField', value=supp_type, lists=lists)

    @data("deviceIndex", "ClientID")
    def test_25_post_txt_supp_list_with_spec_file(self, supp_type):
        files = self.files_utils.make_file("calling list", extension='txt', name="txt_supp_file_spc")
        self.lists_utils.post_suppression_list(upload_file=files,
                                               name='test_25_post_txt_supp_list_with_spec_file' + supp_type,
                                               suppression_type=supp_type, expiration_date='0', spec_id=self.spec_id)
        supp_list = self.conf_server.check_in_cme_by_name(name='test_25_post_txt_supp_list_with_spec_file' + supp_type,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value=supp_type)
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value='0.0.0.0')
        size = len(self.device_utils.get_devices_or_client_from_list(files, list_type=supp_type, l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_25_post_txt_supp_list_with_spec_file' + supp_type,
                                                 parameter='suppressionField', value=supp_type, lists=lists)

    @data("deviceIndex", "ClientID")
    def test_26_post_csv_supp_list_with_spec_file(self, supp_type):
        files = self.files_utils.make_file("calling list", extension='csv', name="csv_supp_file_spc")
        self.lists_utils.post_suppression_list(upload_file=files, spec_id=self.spec_id,
                                               name='test_26_post_csv_supp_list' + supp_type,
                                               suppression_type=supp_type, expiration_date='0')
        supp_list = self.conf_server.check_in_cme_by_name(name='test_26_post_csv_supp_list' + supp_type,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value=supp_type)
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value='0.0.0.0')
        size = len(self.device_utils.get_devices_or_client_from_list(files, list_type=supp_type, l_type="sup"))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name='test_26_post_csv_supp_list' + supp_type, value=supp_type,
                                                 parameter='suppressionField', lists=lists)

    # @data("deviceIndex", "ClientID")
    # def test_27_post_xls_supp_list_with_spec_file(self, supp_type):
    #     files = self.files_utils.make_file("calling list", extension="xls", name="xls_supp_file_spc")
    #     self.lists_utils.post_suppression_list(upload_file=files,
    #                                            name='test_27_post_xls_supp_list_with_spec_file' + supp_type,
    #                                            suppression_type=supp_type, expiration_date='5', spec_id=self.spec_id)
    #     supp_list = self.conf_server.check_in_cme_by_name(name='test_27_post_xls_supp_list_with_spec_file'+supp_type,
    #                                                       object_type='CFGTableAccess')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
    #                                                     value=supp_type)
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
    #                                                     value='5.0.0.0')
    #     size = len(self.device_utils.get_devices_or_client_from_list(files, list_type=supp_type))
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
    #     lists = self.lists_utils.get_suppression_lists()
    #     self.lists_utils.get_list_with_parameter(name='test_27_post_xls_supp_list_with_spec_file' + supp_type,
    #                                              parameter='suppressionField', value=supp_type, lists=lists)
    #
    # @data("deviceIndex", "ClientID")
    # def test_28_post_xlsx_supp_list_with_spec_file(self, supp_type):
    #     files = self.files_utils.make_file("calling list", extension="xlsx", name="xlsx_supp_file_spc")
    #     self.lists_utils.post_suppression_list(upload_file=files,
    #                                            name='test_28_post_xlsx_supp_list_with_spec_file' + supp_type,
    #                                            suppression_type=supp_type, expiration_date='2', spec_id=self.spec_id)
    #     supp_list = self.conf_server.check_in_cme_by_name(name='test_28_post_xlsx_supp_list_with_spec_file'+supp_type,
    #                                                       object_type='CFGTableAccess')
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
    #                                                     value=supp_type)
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
    #                                                     value='2.0.0.0')
    #     size = len(self.device_utils.get_devices_or_client_from_list(files, list_type=supp_type))
    #     self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
    #     lists = self.lists_utils.get_suppression_lists()
    #     self.lists_utils.get_list_with_parameter(name='test_28_post_xlsx_supp_list_with_spec_file' + supp_type,
    #                                              parameter='suppressionField', value=supp_type, lists=lists)

    def test_29_delete_list_while_uploading(self):
        self.lists_utils.post_suppression_list(name='test_29_delete_list_while_uploading', check_in_db=False,
                                               upload_file=self.big_file, suppression_type='deviceIndex',
                                               spec_id=self.spec_id, expiration_date='1', job_check=False)
        supp_id = self.lists_utils.get_supp_list_id_or_parameter('test_29_delete_list_while_uploading')
        sleep(1)
        self.lists_utils.delete_suppression_list(supp_list_id=supp_id, check_in_db=False, expected_code=400)

    @data("deviceIndex", "ClientID")
    def test_30_post_big_suppression_list(self, supp_type):
        self.lists_utils.post_suppression_list(name="test_30_post_big_suppression_list" + supp_type,
                                               expiration_date='3', upload_file=self.big_file,
                                               suppression_type=supp_type, spec_id=self.spec_id)
        supp_list = self.conf_server.check_in_cme_by_name(name="test_30_post_big_suppression_list" + supp_type,
                                                          object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='suppressionType',
                                                        value=supp_type)
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='expirationDuration',
                                                        value='3.0.0.0')
        size = len(set(self.device_utils.get_devices_or_client_from_list(self.big_file, list_type=supp_type,
                                                                         l_type="sup")))
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size', value=str(size))
        lists = self.lists_utils.get_suppression_lists()
        self.lists_utils.get_list_with_parameter(name="test_30_post_big_suppression_list" + supp_type,
                                                 parameter='suppressionField', value=supp_type, lists=lists)

    def test_31_get_list_with_invalid_param_abc(self):
        self.lists_utils.get_suppression_lists(supp_list_id='abc', expected_code=400)

    def test_32_get_list_with_invalid_param_1234567890(self):
        self.lists_utils.get_suppression_lists(supp_list_id='1234567890', expected_code=404)

    def test_33_download_list_with_invalid_param_abc(self):
        self.lists_utils.get_suppression_lists(supp_list_id='abc', expected_code=400, download=True)

    def test_34_download_list_with_invalid_param_1234567890(self):
        self.lists_utils.get_suppression_lists(supp_list_id='1234567890', expected_code=404, download=True)

    def test_35_delete_not_existing_list_with_id_abc(self):
        self.lists_utils.delete_suppression_list(supp_list_id='abc', expected_code=400)

    def test_36_delete_not_existing_list_with_id_11234567890(self):
        self.lists_utils.delete_suppression_list(supp_list_id='1234567890', expected_code=404)

    def test_37_validate_format_of_suppression_records_device(self):
        self.lists_utils.validate_format_of_suppression_records('deviceIndex')

    def test_39_download_rejected_records(self):
        supp_list_id = self.lists_utils.post_suppression_list(upload_file=self.supp_incorrect_value,
                                                              return_id=True, name='test_39_supp',  check_in_db=False,
                                                              suppression_type='deviceIndex', expiration_date='3')
        self.conf_server.check_in_cme_by_name(name='test_39_supp', object_type='CFGTableAccess')
        record = self.device_utils.get_devices_from_file(list_file=self.supp_incorrect_value, incorrect=True)
        ccid = self.api.get_contact_center_id_by_domain(domain='Suppression_Lists')
        self.files_utils.check_artifacts_in_container(list_type='suppression-lists', list_id=supp_list_id,
                                                      index='rejects', ccid=ccid)
        rejected = self.lists_utils.get_suppression_lists(supp_list_id=supp_list_id, download=True,
                                                          index='rejects').content.split(',')
        sort = [i.replace(" ", "") for i in rejected]
        assert sorted(sort) == sorted(record), "Not all record return. Returned records: '{}'".format(rejected)

    def test_40_download_rejected_messages(self):
        supp_list_id = self.lists_utils.post_suppression_list(upload_file=self.import_activity,
                                                              spec_id=self.spec_id,
                                                              return_id=True, name='test_40_supp',  check_in_db=False,
                                                              suppression_type='deviceIndex', expiration_date='3')
        self.conf_server.check_in_cme_by_name(name='test_40_supp', object_type='CFGTableAccess')
        ccid = self.api.get_contact_center_id_by_domain(domain='Suppression_Lists')
        self.files_utils.check_artifacts_in_container(list_type='suppression-lists', list_id=supp_list_id,
                                                      index='msg', ccid=ccid)
        rejected = self.lists_utils.get_suppression_lists(supp_list_id=supp_list_id, download=True,
                                                          index='messages').content
        assert rejected == 'Empty string in field device10, line 20', "Not all record return. Returned" \
                                                                      " records: '{}'".format(rejected)

    def test_41_download_rejected_record_csv_extension(self):
        supp_file = abspath(self.files_utils.make_file("suppression", incorrect=3, supp_list_type='deviceIndex',
                                                       extension='csv', name="test_42_supp"))
        supp_list_id = self.lists_utils.post_suppression_list(upload_file=supp_file, return_id=True,
                                                              name='test_42_supp', check_in_db=False,
                                                              suppression_type='deviceIndex', expiration_date='3')
        supp_list = self.conf_server.check_in_cme_by_name(name='test_42_supp', object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=supp_list, parameter='size',
                                                        value='100')
        record = self.device_utils.get_devices_from_file(list_file=supp_file, incorrect=True)
        ccid = self.api.get_contact_center_id_by_domain(domain='Suppression_Lists')
        self.files_utils.check_artifacts_in_container(list_type='suppression-lists', list_id=supp_list_id,
                                                      index='rejects', ccid=ccid, extension='csv')
        rejected = self.lists_utils.get_suppression_lists(supp_list_id=supp_list_id, download=True,
                                                          index='rejects').content.split(',')
        sort = [i.replace(" ", "") for i in rejected]
        assert sorted(sort) == sorted(record), "Not all record return. Returned records: '{}'".format(rejected)

    def test_42_check_artifacts_in_container_after_deleting_list(self):
        supp_list_id = self.lists_utils.post_suppression_list(upload_file=self.supp_incorrect_value, return_id=True,
                                                              name='test_43_supp',  check_in_db=False,
                                                              suppression_type='deviceIndex', expiration_date='3')
        self.conf_server.check_in_cme_by_name(name='test_43_supp', object_type='CFGTableAccess')
        ccid = self.api.get_contact_center_id_by_domain(domain='Suppression_Lists')
        self.files_utils.check_artifacts_in_container(list_type='suppression-lists', list_id=supp_list_id,
                                                      index='rejects', ccid=ccid)
        self.files_utils.check_artifacts_in_container(list_type='suppression-lists', list_id=supp_list_id, index='msg',
                                                      ccid=ccid)
        self.lists_utils.delete_suppression_list(supp_list_id=supp_list_id)
        sleep(2)
        self.files_utils.check_artifacts_in_container(list_type='suppression-lists', list_id=supp_list_id,
                                                      index='rejects', ccid=ccid, exist=False)
        self.files_utils.check_artifacts_in_container(list_type='suppression-lists', list_id=supp_list_id, index='msg',
                                                      ccid=ccid, exist=False)

    def test_43_download_rejected_records_with_incorrect_tenant(self):
        supp_list_id = self.lists_utils.post_suppression_list(upload_file=self.supp_incorrect_value,
                                                              return_id=True, name='test_41_supp',  check_in_db=False,
                                                              suppression_type='deviceIndex', expiration_date='3')
        self.conf_server.check_in_cme_by_name(name='test_41_supp', object_type='CFGTableAccess')
        env_id = self.api.create_environments()
        ccid = self.api.create_cc_with_domain_if_does_not_exist(environment_id=env_id, domain="qwerty")
        client_credentials_token = self.api.get_client_credentials_token()
        sleep(5)
        self.api.configure_tenant(ccid, env_id, client_credentials_token)
        self.lists_utils.get_suppression_lists(supp_list_id=supp_list_id, download=True, index='rejects',
                                               expected_code=404,
                                               auth={"data": {"domain_username": "qwerty" + "\\" + self.api.user,
                                                     "password": self.api.password}})

    @data("ClientID", "deviceIndex")
    def test_44_post_supp_list_with_short_name(self, supp_type):
        name = self.string_utils.rand_string(2)
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, name=name, spec_id=self.spec_id,
                                               suppression_type=supp_type, expiration_date='3', check_in_db=False,
                                               expected_code=400)
        self.conf_server.check_in_cme_by_name(name=name, object_type='CFGTableAccess', should_exist=False)

    @data("appendOnly", "flushAndAppend")
    def test_45_append_supp_list_device(self, upload_mode):
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                                         name='test_45_append_device' + upload_mode, return_id=True,
                                                         expiration_date="3", suppression_type='deviceIndex')
        self.conf_server.check_in_cme_by_name(name="test_45_append_device" + upload_mode, object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id, list_id=supp_id,
                                               name='test_45_append_device' + upload_mode, expiration_date="3",
                                               suppression_type='deviceIndex', upload_mode=upload_mode)
        self.conf_server.check_created_and_modified_dates_in_annex(name="test_45_append_device" + upload_mode,
                                                                   object_type='CFGTableAccess')

    @data("appendOnly", "flushAndAppend")
    def test_46_append_supp_list_client_id(self, upload_mode):
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                                         name='test_46_append_client_id' + upload_mode, return_id=True,
                                                         expiration_date="3", suppression_type='ClientID')
        self.conf_server.check_in_cme_by_name(name="test_46_append_client_id" + upload_mode,
                                              object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id, list_id=supp_id,
                                               name='test_46_append_client_id' + upload_mode, expiration_date="3",
                                               suppression_type='ClientID', upload_mode=upload_mode)
        self.conf_server.check_created_and_modified_dates_in_annex(name="test_46_append_client_id" + upload_mode,
                                                                   object_type='CFGTableAccess')

    @data("appendOnly", "flushAndAppend", "appendAndUpdate")
    def test_47_append_supp_list_no_auth(self, upload_mode):
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, suppression_type='ClientID',
                                                         name='test_47_append_supp_list_no_auth' + upload_mode,
                                                         return_id=True, expiration_date="3", spec_id=self.spec_id)
        self.conf_server.check_in_cme_by_name(name="test_47_append_supp_list_no_auth" + upload_mode,
                                              object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id, list_id=supp_id,
                                               name='test_47_append_supp_list_no_auth' + upload_mode,
                                               expiration_date="3", suppression_type='ClientID', check_in_db=False,
                                               upload_mode=upload_mode, auth=False, expected_code=401)

    def test_48_negative_append_only_another_suppression_type_supp_list(self):
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                                         name='test_48_append_only_supp_list', return_id=True,
                                                         expiration_date="3", suppression_type='ClientID')
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                               name='test_48_append_only_supp_list',
                                               expiration_date="3", suppression_type='deviceIndex', list_id=supp_id,
                                               upload_mode="appendOnly", check_in_db=False, expected_code=400)

    def test_49_negative_append_only_another_suppression_type_supp_list(self):
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                                         name='test_49_append_only_supp_list', return_id=True,
                                                         expiration_date="3", suppression_type='deviceIndex')
        self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                               name='test_49_append_only_supp_list',
                                               expiration_date="3", suppression_type='ClientID', list_id=supp_id,
                                               upload_mode="appendOnly", check_in_db=False, expected_code=400)

    def test_50_negative_get_suppression_list_without_read_access(self):
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, spec_id=self.spec_id,
                                                         name='test_50_get_suppression_list', return_id=True,
                                                         expiration_date="3", suppression_type='deviceIndex')
        self.api.get_object_without_read_access(required_object="suppression-list/" + str(supp_id))

    @data("ClientID", "deviceIndex")
    def test_51_post_list_with_not_existing_spec_file(self, supp_type):
        name = self.string_utils.rand_string(5)
        resp = self.lists_utils.post_suppression_list(upload_file=self.supp_file, name=name, spec_id="123456789",
                                                      suppression_type=supp_type, expiration_date='3',
                                                      check_in_db=False, expected_code=404)
        message = "Failed to load specification Id=123456789"
        assert message == (resp['status']['message']), "Failed to load specification Id=123456789"

    @data("ClientID", "deviceIndex")
    def test_52_post_list_with_incorrect_spec_file(self, supp_type):
        name = self.string_utils.rand_string(5)
        resp = self.lists_utils.post_suppression_list(upload_file=self.supp_file, name=name, spec_id="abcde",
                                                      suppression_type=supp_type, expiration_date='3',
                                                      check_in_db=False, expected_code=404)
        message = "Failed to load specification Id=abcde"
        assert message == (resp['status']['message']), "Failed to load specification Id=abcde"

    def test_53_check_default_CXContactSMSOptOut_suppression_list(self):
        self.lists_utils.get_suppression_lists(name="CXContactSMSOptOut")
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.conf_server.get_object_property_from_annex(object_from_cme=resp, parameter='suppressionChannels',
                                                        value=['sms'])
        self.conf_server.get_object_property_from_annex(object_from_cme=resp, parameter='suppressionType',
                                                        value='deviceIndex')
        self.conf_server.get_object_property_from_annex(object_from_cme=resp, parameter='required',
                                                        value='true')
        self.conf_server.get_object_property_from_annex(object_from_cme=resp, parameter='expirationDuration',
                                                        value='-1.0.0.0')

    @data("appendOnly", "flushAndAppend")
    def test_54_append_file_to_default_CXContactSMSOptPut(self, upload_mode):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id, required="true",
                                               list_id=resp.DBID, name="CXContactSMSOptOut", expiration_date=-1,
                                               suppression_type='deviceIndex', upload_mode=upload_mode)

    def test_55_negative_put_default_CXContactSMSOptPut_by_another_type(self):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.put_suppression_list(supp_list_id=resp.DBID, expiration_date=-1, expected_code=400,
                                              supp_type="ClientID")

    @expected_failure("For now put request for change mandatory values in default supplist returns status code - 200")
    def test_56_negative_put_default_CXContactSMSOptPut_by_expiration_date(self):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.put_suppression_list(supp_list_id=resp.DBID, expected_code=400, required=True,
                                              expiration_date=5)

    @expected_failure("For now put request for change mandatory values in default supplist returns status code - 200")
    def test_57_negative_put_default_CXContactSMSOptPut_by_not_required(self):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.put_suppression_list(supp_list_id=resp.DBID, expiration_date=-1, expected_code=400)

    def test_58_negative_put_default_CXContactSMSOptPut_by_channel(self):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.put_suppression_list(supp_list_id=resp.DBID, expected_code=400, required=True,
                                              expiration_date=-1, channel=['voice'])

    @data("appendOnly", "flushAndAppend")
    def test_59_negative_put_with_file_and_another_channel_CXContactSMSOptPut(self, upload_mode):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id, required='true',
                                               list_id=resp.DBID, name="CXContactSMSOptOut", expiration_date=-1,
                                               suppression_type='deviceIndex', upload_mode=upload_mode,
                                               channel=["voice"], expected_code=400, job_check=False, check_in_db=False)

    @expected_failure("For now put request for change mandatory values in default supplist returns status code - 200")
    @data("appendOnly", "flushAndAppend")
    def test_60_negative_put_with_file_and_expiration_date_CXContactSMSOptPut(self, upload_mode):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id, required='true',
                                               list_id=resp.DBID, name="CXContactSMSOptOut", expiration_date=5,
                                               suppression_type='deviceIndex', upload_mode=upload_mode,
                                               expected_code=400, job_check=False, check_in_db=False)

    @expected_failure("For now put request for change mandatory values in default supplist returns status code - 200")
    @data("appendOnly", "flushAndAppend")
    def test_61_negative_put_with_file_and_not_required_CXContactSMSOptPut(self, upload_mode):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id,
                                               list_id=resp.DBID, name="CXContactSMSOptOut", expiration_date=-1,
                                               suppression_type='deviceIndex', upload_mode=upload_mode,
                                               expected_code=400, job_check=False, check_in_db=False)

    @data("appendOnly", "flushAndAppend")
    def test_62_negative_put_with_file_and_another_supp_type_CXContactSMSOptPut(self, upload_mode):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.post_suppression_list(upload_file=self.file_for_append, spec_id=self.spec_id, required='true',
                                               list_id=resp.DBID, name="CXContactSMSOptOut", expiration_date=-1,
                                               suppression_type='ClientID', upload_mode=upload_mode, expected_code=400,
                                               job_check=False, check_in_db=False)

    @expected_failure("For now put request for change mandatory values in default supplist returns status code - 200")
    def test_63_negative_put_default_CXContactSMSOptPut_by_name(self):
        resp = self.conf_server.check_in_cme_by_name(name="CXContactSMSOptOut", object_type='CFGTableAccess')
        self.lists_utils.put_suppression_list(supp_list_id=resp.DBID, expected_code=400, required=True,
                                              expiration_date=-1, name=self.string_utils.rand_string(6))

    @data("ClientID", "deviceIndex")
    def test_64_add_contact_to_suppression_list_from_till(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)

        value = sample(self.device_utils.get_devices_or_client_from_list(
            self.supp_file, 'ClientID' if supp_type == 'ClientID' else 'deviceIndex', self.spec_file,
            l_type='supp'), 1)
        if supp_type == 'ClientID':
            self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value[0], froms=12345,
                                                      until=84500, appendOnly=False)
        else:
            self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value[0],  froms=12345,
                                                      until=84500, appendOnly=False)

    @data("ClientID", "deviceIndex")
    def test_65_negative_add_the_same_contact_to_suppression_list(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)
        value = str(self.device_utils.fake.ssn()) if supp_type == 'ClientID' \
            else str(self.device_utils.generate_number())
        if supp_type == 'ClientID':
            self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value)
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value,
                                                             expected_code=400)
            message = "Cannot store suppression entry. APPEND_ONLY: Duplicated clientId={}, rejected".format(value)
            self.string_utils.assert_message_from_response(resp, expected_message=message)
        else:
            self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value)
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value,
                                                             expected_code=400)
            message = "Cannot store suppression entry. No device(s) except duplicates, rejected"
            self.string_utils.assert_message_from_response(resp, expected_message=message)

    @data("ClientID", "deviceIndex")
    def test_66_add_contact_to_sl_till_from_more_then_24_hours(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)
        value = str(self.device_utils.fake.ssn()) if supp_type == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type == 'ClientID':
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value,
                                                             froms=12345, until=87000, expected_code=400)
        else:
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value,
                                                             froms=12345, until=87000, expected_code=400)
        self.string_utils.assert_message_from_response(resp, expected_message="Invalid Till value. "
                                                                              "Must be less then or equal 24 hours")

    @data("ClientID", "deviceIndex")
    def test_67_add_contact_to_sl_till_from_more_then_till(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)
        value = str(self.device_utils.fake.ssn()) if supp_type == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type == 'ClientID':
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value,
                                                             froms=55, until=5, expected_code=400)
        else:
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value,
                                                             froms=55, until=5, expected_code=400)
        self.string_utils.assert_message_from_response(resp, expected_message="Invalid From/Till values. "
                                                                              "From cannot be greater then Till")

    @data("ClientID", "deviceIndex")
    def test_68_add_contact_to_sl_with_negative_from_value(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)
        value = str(self.device_utils.fake.ssn()) if supp_type == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type == 'ClientID':
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value,
                                                             froms=-5, until=5, expected_code=400)
        else:
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value,
                                                             froms=-5, until=5, expected_code=400)
        self.string_utils.assert_message_from_response(resp, expected_message="Invalid From/Till values. "
                                                                              "Cannot be negative")

    @data("ClientID", "deviceIndex")
    def test_69_add_contact_to_sl_with_negative_till_value(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)
        value = str(self.device_utils.fake.ssn()) if supp_type == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type == 'ClientID':
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value,
                                                             froms=1234, until=-5, expected_code=400)
        else:
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value,
                                                             froms=1234, until=-5, expected_code=400)
        self.string_utils.assert_message_from_response(resp, expected_message="Invalid From/Till values. "
                                                                              "Cannot be negative")

    @data("ClientID", "deviceIndex")
    def test_70_add_contact_to_sl_invalid_from_till(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)
        value = str(self.device_utils.fake.ssn()) if supp_type == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type == 'ClientID':
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value,
                                                             froms="abcde", until=1234, expected_code=400)
            self.string_utils.assert_message_from_response(resp, expected_message=["/fields/from: should be number"])
        else:
            resp = self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value,
                                                             froms=1234, until="abcde", expected_code=400)
            self.string_utils.assert_message_from_response(resp, expected_message=["/fields/until: should be number"])

    @data("ClientID", "deviceIndex")
    def test_71_post_sl_with_incorrect_from_till_in_file(self, supp_type):
        name = self._testMethodName
        supp_file = abspath('api_aggregator/files/incorrect_from_till.txt')
        supp_id = self.lists_utils.post_suppression_list(upload_file=supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type, check_in_db=False,
                                                         spec_id=self.spec_id, return_id=True)
        rejected = self.lists_utils.get_suppression_lists(supp_list_id=supp_id, download=True,
                                                          index='messages').content
        assert rejected == 'Invalid From/Till values. From cannot be ' \
                           'greater then Till', "Not all record return. Returned records: '{}'".format(rejected)

    @data("ClientID", "deviceIndex")
    def test_72_add_contact_to_sl_with_from_value_equal_to_till(self, supp_type):
        name = self._testMethodName
        supp_id = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date="3",
                                                         name=name, suppression_type=supp_type,
                                                         spec_id=self.spec_id, return_id=True)
        value = str(self.device_utils.fake.ssn()) if supp_type == 'ClientID' else str(
            self.device_utils.generate_number())
        if supp_type == 'ClientID':
            self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, client_id=value, froms=12345,
                                                      until=12345)
        else:
            self.lists_utils.add_contact_to_supp_list(list_id=supp_id, list_name=name, device=value,  froms=12345,
                                                      until=12345)
