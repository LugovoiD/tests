from os.path import abspath
from api_utils.outbound_test_case import OutboundBaseTestApi
from api_utils.lists_utils import ListsUtils
from api_utils.campaign_utils import CampaignUtils
from ddt import ddt, data
from api_utils.utils import expected_failure


@ddt
class TestOutboundAPIReadOnly(OutboundBaseTestApi):
    @classmethod
    def setUpClass(cls):
        super(TestOutboundAPIReadOnly, cls).setUpClass()
        cls.lists_utils = ListsUtils()
        cls.campaign_utils = CampaignUtils()
        cls.files_utils = cls.api.file_utils
        cls.string_utils = cls.api.string_utils
        cls.file_name = cls.files_utils.make_file("calling list", name="cl_read_only")
        cls.supp_file = cls.files_utils.make_file("calling list", name="cl_for_supp", records_count=50)
        cls.spec_file = abspath('api_aggregator/files/list_spec.spc')
        cls.spec_id = cls.lists_utils.post_specifications(name="spc_for_cl_read_only", upload_file=cls.spec_file)
        cls.list_id = cls.lists_utils.post_list(upload_file=cls.file_name, name='read_only_calling',
                                                spec_id=cls.spec_id)
        cls.supp_list = cls.lists_utils.post_suppression_list(upload_file=cls.supp_file, name='read_only_suppression',
                                                              suppression_type='deviceIndex', expiration_date='3',
                                                              spec_id=cls.spec_id, return_id=True)
        cls.dialing_profile = cls.campaign_utils.create_dialing_profile(name='dialing_profile_read_only',
                                                                        groupName='AG_Test_2')

        cls.lists_utils.user = cls.lists_utils.password = "User"
        cls.lists_utils.cookies = False

        cls.campaign_utils.api.user = cls.campaign_utils.api.password = "User"
        cls.campaign_utils.api.cookies = False

    @expected_failure("Fix in CLOUDCON-6668 ")
    def test_01_post_list_read_only(self):
        list = self.lists_utils.post_list(upload_file=self.file_name, name=self.string_utils.rand_string(5),
                                          spec_id=self.spec_id, expected_code=[403, 603], check_in_cme=False,
                                          return_response=True)
        message = "Unable to find/create list. Error: Unable to find/create list. Error: EventError code=[7542281] " \
                  "received: Insufficient permissions to perform this operation"
        self.string_utils.assert_message_from_response(list, expected_message=message)

    @expected_failure("Fix in CLOUDCON-6668 ")
    def test_02_delete_list_read_only(self):
        delete = self.lists_utils.delete_lists('lists', self.list_id, expected_code=[403, 603], check_in_cme=False,
                                               check_in_db=False)
        message = "Insufficient permissions to perform this operation"
        self.string_utils.assert_message_from_response(delete, expected_message=message)

    @expected_failure("Fix in CLOUDCON-6668 ")
    @data("deviceIndex", "ClientID")
    def test_03_post_supp_list_read_only(self, supp_type):
        name = self.string_utils.rand_string(5)
        sup_list = self.lists_utils.post_suppression_list(upload_file=self.supp_file, expiration_date='3',
                                                          name=name + supp_type, suppression_type=supp_type,
                                                          spec_id=self.spec_id, expected_code=[403, 603],
                                                          check_in_db=False)
        message = 'Unable to find/create list. Error: EventError code=[7542281] received: Insufficient permissions ' \
                  'to perform this operation'
        assert message == sup_list['status']['message'], "Expected error message {0} not equal to " \
                                                         "actual {1}".format(message, sup_list['status']['message'])

    @expected_failure("Fix in CLOUDCON-6668 ")
    def test_04_delete_supp_list_read_only(self):
        delete_supp = self.lists_utils.delete_suppression_list(supp_list_id=self.supp_list, expected_code=[403, 603],
                                                               check_in_db=False)
        message = "Insufficient permissions to perform this operation"
        self.string_utils.assert_message_from_response(delete_supp, expected_message=message)

    @expected_failure("Fix in CLOUDCON-6668 ")
    def test_05_post_dialing_profile_read_only(self):
        dp = self.campaign_utils.create_dialing_profile(name=self.string_utils.rand_string(5), groupName='AG_Test_2',
                                                        expected_code=[403, 603], return_response=True)
        message = "EventError code=[7542281] received: Insufficient permissions to perform this operation"
        self.string_utils.assert_message_from_response(dp, expected_message=message)

    @expected_failure("Fix in CLOUDCON-6668 ")
    def test_06_delete_dialing_profile_read_only(self):
        delete_dp = self.campaign_utils.delete_campaign_objects(obj='dialing-profiles', dbid=self.dialing_profile,
                                                                expected_code=[403, 603])
        message = "EventError code=[2231305] received: Insufficient permissions to perform this operation"
        self.string_utils.assert_message_from_response(delete_dp, message)
