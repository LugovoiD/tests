from ddt import ddt, data
from api_utils.campaign_utils import CampaignUtils
from api_utils.cm_utils import CMUtils
from api_utils.lists_utils import ListsUtils
from api_utils.compliance_utils import ComplianceUtils
from api_utils.outbound_test_case import OutboundBaseTestApi
from api_utils.utils import *


@ddt
class TestOutboundAPICampaign(OutboundBaseTestApi):
    @classmethod
    def setUpClass(cls):
        super(TestOutboundAPICampaign, cls).setUpClass()
        cls.string_utils = cls.api.string_utils
        cls.db_utils = cls.api.db_utils
        cls.cm_utils = CMUtils()
        cls.campaign_utils = CampaignUtils()
        cls.compliance_utils = ComplianceUtils()
        cls.lists_utils = ListsUtils()
        cls.spec_file = os.path.abspath("api_aggregator/files/list_spec.spc")
        cls.spec_id = cls.lists_utils.post_specifications(name="spc_for_campaign", upload_file=cls.spec_file)
        cls.supp_file = os.path.abspath("api_aggregator/files/supp_list_elastic.txt")
        cls.file_ = os.path.abspath("api_aggregator/files/call_list_elastic.csv")
        cls.list_id = cls.lists_utils.post_list(upload_file=cls.file_, name="CL_for_preload", spec_id=cls.spec_id)

        for i in range(3):
            cls.file_name = os.path.abspath(
                cls.lists_utils.file_utils.make_file(list_type="calling list", extension='txt',
                                                     records_count=5))
            cls.lists_utils.post_list(upload_file=cls.file_name, name='CL_Test_{}'.format(i),
                                      description='CL for campaign',
                                      spec_id=cls.spec_id, check_in_db=False)
            cls.sup_f = os.path.abspath(
                cls.lists_utils.file_utils.make_file(list_type="suppression", supp_list_type="deviceIndex",
                                                     extension='txt', records_count=5))
            cls.lists_utils.post_suppression_list(upload_file=cls.sup_f, name="SL_Test_{}".format(i),
                                                  suppression_type='deviceIndex', expiration_date='3')

        cls.campaign_utils.create_session_profile('SessionProfile')
        modes = ['Preview', 'Predict', 'Progress']
        [cls.campaign_utils.create_dialing_profile(name='DialingProfile{}'.format(item), groupName='AG_Test_1',
                                                   dialMode=item, CPNDigits=12345) for item in modes]
        cls.campaign_utils.create_dialing_profile(name='DialingProfileIVR', groupName='AG_Test_1', ivrMode=True,
                                                  dialMode='Progress', CPNDigits=12345, optMethod="OverdialRate",
                                                  optMethodValue=2)
        cls.ivr_templ = cls.campaign_utils.create_campaign_template(name='CampaignTemplateIVR',
                                                                    dialing_profile_name='DialingProfileIVR')
        cls.templ_predictive = cls.campaign_utils.create_campaign_template(name='CampaignTemplatePredictive',
                                                                           dialing_profile_name='DialingProfilePredict')
        cls.templ_id = cls.campaign_utils.create_campaign_template(name='Campaign_Template',
                                                                   dialing_profile_name='DialingProfilePreview')
        cls.templ = cls.campaign_utils.create_campaign_template(name='CampaignTemplateProgressive',
                                                                dialing_profile_name='DialingProfileProgress')
        cls.preload_call_list_name = os.path.abspath("campaign_manager/files/CL_DNC.txt")
        cls.preload_list_id = cls.lists_utils.post_list(upload_file=cls.preload_call_list_name, name='Location_preload',
                                                        description='CL for campaign', spec_id=cls.spec_id)

    def test_101_check_preload_with_mandatory_sup_list(self):
        sup_id = self.lists_utils.post_suppression_list(name="SupForPreloadMandatory", upload_file=self.supp_file,
                                                        suppression_type='deviceIndex',
                                                        expiration_date=5, return_id=True, required='true')
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ_id,
                                                         callingLists=[{'name': 'CL_for_preload', 'weight': 10}])
        name = resp['name']
        dbid = resp['DBID']
        self.campaign_utils.campaign_start_pause_stop_resume(action='start', campaign_name=name[0],
                                                             group_name=name[1])
        self.campaign_utils.check_preload_campaign(calling_lists=[self.list_id], suppression_lists=[sup_id],
                                                   camp_group_dbid=dbid, reason=1)
        self.lists_utils.delete_suppression_list(sup_id)

    def test_102_reserve_agents_in_campaign_group(self):
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                         campaignTemplateId=self.templ, dialMode='Progress')
        dbid = resp['DBID']
        name_group = resp['name'][0] + "@" + resp['name'][1]
        self.campaign_utils.edit_campaign_group_by_id(dbid=dbid, groupName="AG_Test_1", campaignTemplateId=self.templ,
                                                      callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                      dialMode="Progress", reserveAgents=3)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(name=name_group,
                                                                          object_type='CFGCampaignGroup')
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj,
                                                                    parameter='progressive_blending_reserved_agents',
                                                                    value=3, section="OCServer")

    def test_104_complete_if_no_more_records(self):
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                         campaignTemplateId=self.templ_id, dialMode='Preview',
                                                         completeIfNoMoreRecords=True)
        self.campaign_utils.campaign_start_pause_stop_resume(action='start', campaign_name=resp['name'][0],
                                                             group_name=resp['name'][1])
        time.sleep(3)
        self.campaign_utils.check_campaign_status(campaign_name=resp['name'][0], group_name=resp['name'][1],
                                                  expected_result="NotLoaded")

    def test_106_edit_campaign_group_with_letter_in_predictive_min_overdial_rate(self):
        camp_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                               callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                               campaignTemplateId=self.templ_predictive,
                                                               CPNDigits=12345, dialMode='Predict',
                                                               paAbandonRateLimit="30", optMethod="BusyFactor",
                                                               optMethodValue="70", engageAgentsFirst=True)
        dbid = camp_group['DBID']
        self.campaign_utils.edit_campaign_group_by_id(dbid=dbid, groupName="AG_Test_1",
                                                      callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                      campaignTemplateId=self.templ_predictive, CPNDigits=12345,
                                                      predictiveMinOverdialRate="v", dialMode='Predict',
                                                      paAbandonRateLimit="30", optMethod="BusyFactor",
                                                      optMethodValue="70",
                                                      smallGroupSize="", engageAgentsFirst=True, expected_code=400)

    def test_107_edit_delivery_options(self):
        delivery = {"AM": "1010", "Fax": "1010", "Silence": "1010"}
        dndbid = self.campaign_utils.get_dn_dbid_by_name("1010")
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                         campaignTemplateId=self.templ_id, delivery=delivery)
        self.lists_utils.conf_server.check_campaign_group_treatment(template_name=resp['name'][0],
                                                                    agent_name=resp['name'][1],
                                                                    delivery=delivery, dndbid=dndbid)
        new_delivery = {"AM": "3501", "Fax": "3501", "Silence": "3501"}
        dndbid = self.campaign_utils.get_dn_dbid_by_name("3501")
        self.campaign_utils.edit_campaign_group_by_id(dbid=resp['DBID'], campaignTemplateId=self.templ_id,
                                                      callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                      delivery=new_delivery)
        self.lists_utils.conf_server.check_campaign_group_treatment(template_name=resp['name'][0],
                                                                    agent_name=resp['name'][1],
                                                                    delivery=new_delivery, dndbid=dndbid)

    def test_108_edit_retry_options(self):
        retry_opts = self.campaign_utils.generate_retry_options()
        dbid = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                         campaignTemplateId=self.templ_id, applyToRecord=retry_opts)
        self.lists_utils.conf_server.check_retry_options_treatments(retry_opts=retry_opts, dbid=dbid['DBID'])
        new_retry_opts = self.campaign_utils.generate_retry_options()
        self.campaign_utils.edit_campaign_group_by_id(callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                      dbid=dbid['DBID'],
                                                      campaignTemplateId=self.templ_id, applyToRecord=new_retry_opts)
        self.lists_utils.conf_server.check_retry_options_treatments(retry_opts=new_retry_opts, dbid=dbid['DBID'])

    def test_109_edit_joint_treatments_remove_retry_options(self):
        delivery = {"AM": "3501", "Fax": "3501", "Silence": "3501"}
        dndbid = self.campaign_utils.get_dn_dbid_by_name("3501")
        retry_opts = self.campaign_utils.generate_retry_options(custom_call_results=['AnsweringMachine', 'FaxDetected',
                                                                                     'Silence'])
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', delivery=delivery,
                                                         callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                         campaignTemplateId=self.templ_id, applyToRecord=retry_opts)
        self.lists_utils.conf_server.check_campaign_group_treatment(template_name=resp['name'][0],
                                                                    agent_name=resp['name'][1],
                                                                    delivery=delivery, dndbid=dndbid)
        new_delivery = {"Fax": "3501"}
        del retry_opts[0]
        self.campaign_utils.edit_campaign_group_by_id(callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                      dbid=resp['DBID'],
                                                      campaignTemplateId=self.templ_id, delivery=new_delivery,
                                                      applyToRecord=retry_opts)
        self.lists_utils.conf_server.check_campaign_group_treatment(template_name=resp['name'][0],
                                                                    agent_name=resp['name'][1],
                                                                    delivery=new_delivery, dndbid=dndbid)
        self.lists_utils.conf_server.check_retry_options_treatments(retry_opts=retry_opts, dbid=resp['DBID'])

    def test_110_edit_joint_treatments(self):
        delivery = {"AM": "3501", "Fax": "3501", "Silence": "3501"}
        dndbid = self.campaign_utils.get_dn_dbid_by_name("3501")
        retry_opts = self.campaign_utils.generate_retry_options(custom_call_results=['AnsweringMachine', 'FaxDetected',
                                                                                     'Silence'])
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', delivery=delivery,
                                                         callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                         campaignTemplateId=self.templ_id, applyToRecord=retry_opts)
        self.lists_utils.conf_server.check_campaign_group_treatment(template_name=resp['name'][0],
                                                                    agent_name=resp['name'][1],
                                                                    delivery=delivery, dndbid=dndbid)
        new_delivery = {"AM": "3501"}
        new_retry_opts = self.campaign_utils.generate_retry_options(custom_call_results=['AnsweringMachine'])
        self.campaign_utils.edit_campaign_group_by_id(callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                      dbid=resp['DBID'],
                                                      campaignTemplateId=self.templ_id, delivery=new_delivery,
                                                      applyToRecord=new_retry_opts)
        self.lists_utils.conf_server.check_campaign_group_treatment(template_name=resp['name'][0],
                                                                    agent_name=resp['name'][1],
                                                                    delivery=new_delivery, dndbid=dndbid)
        self.lists_utils.conf_server.check_retry_options_treatments(retry_opts=new_retry_opts, dbid=resp['DBID'])

    @data('Progress', 'Predict', 'Power')
    def test_111_create_campaign_group_with_ivr_mode(self, dial_mode):
        opt_method = 0 if dial_mode is 'Progress' else 2
        dial_modes_map = {'Progress': '9', 'Predict': '10', 'Power': '11'}
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', ivrMode=True,
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         campaignTemplateId=self.ivr_templ, dialMode=dial_mode,
                                                         optMethod="OverdialRate", optMethodValue=opt_method,
                                                         callAnswerTypeRecognition="accurate_am_detection",
                                                         callTimeGuardTimeout=10000)
        self.lists_utils.conf_server.check_campaign_group_in_cme(template_name=resp['name'][0], agent_name="AG_Test_1",
                                                                 dial_mode=dial_modes_map[dial_mode], eng_agents=False)

    @data('telephony_preset', 'accurate_am_detection', 'no_am_detection', 'positive_am_detection',
          'full_positive_am_detection', 'no_progress_detection')
    def test_112_create_campaign_group_ivr_with_answer_type_recognition(self, answ_type_recogn):
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         ivrMode=True,
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         campaignTemplateId=self.ivr_templ, dialMode='Power',
                                                         callAnswerTypeRecognition=answ_type_recogn,
                                                         callTimeGuardTimeout=10000, optMethod="OverdialRate",
                                                         optMethodValue=2)
        self.lists_utils.conf_server.check_campaign_group_in_cme(template_name=resp['name'][0], agent_name="AG_Test_1",
                                                                 dial_mode='11', answ_type_recogn=answ_type_recogn,
                                                                 timeguard_timeout='10000')

    @data('BusyFactor', 'WaitTime', 'DistributionTime')
    def test_113_create_campaign_group_ivr_invalid_optimization_parameter_for_power_and_progressive(self, opt_method):
        self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                  callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                  ivrMode=True, campaignTemplateId=self.ivr_templ, dialMode='Power',
                                                  optMethod=opt_method, expected_code=400)

    def test_114_create_campaign_group_with_number_of_IVR_ports(self):
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', dialMode="Predict",
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         ivrMode=True, campaignTemplateId=self.ivr_templ,
                                                         numOfChannels=20)
        self.lists_utils.conf_server.check_campaign_group_in_cme(template_name=resp['name'][0], agent_name="AG_Test_1",
                                                                 dial_mode='10', num_of_channels=20)

    def test_115_negative_create_camp_group_with_predictive_long_calls_truncation_in_Preview_dial_mode(self):
        self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                  callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                  campaignTemplateId=self.templ_id, dialMode="Preview",
                                                  predictiveLongcallsTruncation=True, expected_code=400)

    def test_116_negative_create_camp_group_with_inbound_call_duration_in_Preview_dial_mode(self):
        self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                  callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                  campaignTemplateId=self.templ_id, dialMode="Preview",
                                                  avgInbCallDuration=300, expected_code=400)

    def test_117_negative_create_camp_group_with_outbound_call_duration_in_Preview_dial_mode(self):
        self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                  callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                  campaignTemplateId=self.templ_id, dialMode="Preview",
                                                  avgOtbCallDuration=300, expected_code=400)

    def test_118_negative_create_campaign_group_with_string_value_of_number_of_IVR_ports(self):
        self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                  callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                  ivrMode=True, campaignTemplateId=self.ivr_templ, dialMode="Predict",
                                                  numOfChannels="inv", expected_code=400)

    @data(['WaitTime', 3], ['DistributionTime', 4], ['OverdialRate', 2], ['BusyFactor', 1])
    def test_119_create_predict_ivr_campaign_group_with_different_opt_method(self, opt_method):
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         ivrMode=True, campaignTemplateId=self.templ_predictive,
                                                         dialMode="Predict", optMethodValue=2, optMethod=opt_method[0])
        self.lists_utils.conf_server.check_campaign_group_in_cme(template_name=resp['name'][0], opt_goal=2,
                                                                 agent_name="AG_Test_1",
                                                                 dial_mode='10', opt_method=opt_method[1])

    def test_120_create_campaign_group_ivr_with_abandon_rate_limit(self):
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         ivrMode=True, campaignTemplateId=self.templ_predictive,
                                                         dialMode="Predict", optMethodValue=2, optMethod="BusyFactor")
        self.lists_utils.conf_server.check_campaign_group_in_cme(template_name=resp['name'][0], opt_goal=2,
                                                                 agent_name="AG_Test_1",
                                                                 dial_mode='10', opt_method=1)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(name=resp['name'][0] + "@" + resp['name'][1],
                                                                          object_type="CFGCampaignGroup")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj,
                                                                    parameter="pa-abandon-rate-limit",
                                                                    section="OCServer", value="3.1")

    @data('Preview', 'PushPreview')
    def test_121_create_campaign_group_Preview_with_ivr_mode(self, dial_mode):
        dial_modes_map = {'Preview': '3', 'PushPreview': '8'}
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         ivrMode=True,
                                                         campaignTemplateId=self.ivr_templ, dialMode=dial_mode,
                                                         callAnswerTypeRecognition="accurate_am_detection",
                                                         callTimeGuardTimeout=10000)
        self.lists_utils.conf_server.check_campaign_group_in_cme(template_name=resp['name'][0], agent_name="AG_Test_1",
                                                                 dial_mode=dial_modes_map[dial_mode])
        self.campaign_utils.get_campaign_group(dbid=resp["DBID"])

    def test_122_edit_campaign_group_by_empty_destinationDN(self):
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         campaignTemplateId=self.templ_id)
        self.campaign_utils.edit_campaign_group_by_id(dbid=resp["DBID"], groupName='AG_Test_1',
                                                      callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                      campaignTemplateId=self.templ_id, destinationDN=None)
        check_dest_DN = self.campaign_utils.get_campaign_group(dbid=resp["DBID"]).json()["data"][
            "destinationDN"]
        assert check_dest_DN is not None, "Value of 'destinationDN' is None"

    @data('Progress', 'Predict', 'Power')
    def test_123_edit_campaign_group_by_change_dial_mode_ivr(self, dial_mode):
        opt_method = 0 if dial_mode is 'Progress' else 2
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         campaignTemplateId=self.ivr_templ, dialMode='Preview',
                                                         optMethod="OverdialRate", optMethodValue=2,
                                                         callAnswerTypeRecognition="accurate_am_detection",
                                                         callTimeGuardTimeout=10000, ivrMode=True)
        self.campaign_utils.edit_campaign_group_by_id(dbid=resp["DBID"], groupName='AG_Test_1',
                                                      callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                      campaignTemplateId=self.ivr_templ, dialMode=dial_mode,
                                                      optMethod="OverdialRate", optMethodValue=opt_method,
                                                      callAnswerTypeRecognition="accurate_am_detection",
                                                      callTimeGuardTimeout=10000, ivrMode=True)
        dial_modes_map = {'Progress': '9', 'Predict': '10', 'Power': '11'}
        self.lists_utils.conf_server.check_campaign_group_in_cme(template_name=resp['name'][0], agent_name="AG_Test_1",
                                                                 dial_mode=dial_modes_map[dial_mode])

    @data('Progress', 'Power')
    def test_124_negative_edit_campaign_group_unallowable_optMethod(self, dial_mode):
        opt_method = 0 if dial_mode is 'Progress' else 2
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         ivrMode=True,
                                                         campaignTemplateId=self.ivr_templ, dialMode=dial_mode,
                                                         optMethod="OverdialRate", optMethodValue=opt_method,
                                                         callAnswerTypeRecognition="accurate_am_detection",
                                                         callTimeGuardTimeout=10000)
        self.campaign_utils.edit_campaign_group_by_id(dbid=resp["DBID"], groupName='AG_Test_1',
                                                      callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                      ivrMode=True, campaignTemplateId=self.ivr_templ,
                                                      dialMode=dial_mode,
                                                      optMethod="BusyFactor", optMethodValue=opt_method,
                                                      expected_code=400,
                                                      callAnswerTypeRecognition="accurate_am_detection",
                                                      callTimeGuardTimeout=10000)

    def test_126_check_that_user_properties_of_session_profile_are_propagated_to_Campaign_Group(self):
        self.campaign_utils.create_session_profile(profile_name="session_profile_default_false", default=False)
        templ_id = self.campaign_utils.create_campaign_template(dialing_profile_name="DialingProfilePredict",
                                                                session_profile_name="session_profile_default_false",
                                                                name="template_with_session_default_false")
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         campaignTemplateId=templ_id, dialMode='Predict')
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(name=resp["name"][0],
                                                                          object_type="CFGCampaign")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter="record_processed",
                                                                    value='true',
                                                                    section="OCServer")
        obj_1 = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=resp["name"][0] + "@" + resp["name"][1],
            object_type="CFGCampaignGroup")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj_1, parameter="record_processed",
                                                                    value='true',
                                                                    section="OCServer")
        obj_2 = self.lists_utils.conf_server.return_object_from_cme_by_name(name="CL_Test_2",
                                                                            object_type="CFGCallingList")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj_2, parameter="record_processed",
                                                                    value='true',
                                                                    section="OCServer")

    def test_128_edit_list_weighting_in_campaign_group(self):
        templ_id = self.campaign_utils.create_campaign_template(name="test_for_edit_list_weight_in_campaign",
                                                                dialing_profile_name="DialingProfilePreview")
        edit_camp_group = self.campaign_utils.create_campaign_group(groupName="AG_Test_1", CPNDigits=12345,
                                                                    callingLists=[{'name': 'CL_Test_1', 'weight': 10}],
                                                                    campaignTemplateId=templ_id)
        dbid = edit_camp_group['DBID']
        weight = randint(1, 100)
        list_weighting = int(round((weight / 10) * 10))
        self.campaign_utils.edit_campaign_group_by_id(dbid=dbid, groupName="AG_Test_2",
                                                      callingLists=[{'name': 'CL_Test_1', 'weight': list_weighting}],
                                                      campaignTemplateId=templ_id)
        name = edit_camp_group['response'].json()['data']['name']
        response = self.lists_utils.conf_server.return_object_from_cme_by_name(name=name.split("@")[0],
                                                                               object_type="CFGCampaign")
        obj = response.__dict__["callingLists"][0].__dict__
        assert obj["share"] == list_weighting, \
            "Incorrect property of callingLists is stored in CME, expected value is" \
            " 'share':{0} but actual value is 'share':{1} ".format(list_weighting, obj["share"])

    def test_129_create_campaign_group_with_incorrect_lists_weighting_value(self):
        templ_id = self.campaign_utils.create_campaign_template(
            name="create_campaign_group_with_incorrect_lists_weighting",
            dialing_profile_name="DialingProfilePreview")

        r = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                      callingLists=[{'name': 'CL_Test_2', 'weight': 200}],
                                                      campaignTemplateId=templ_id, dialMode='Preview',
                                                      expected_code=400)

        message = "Failed to create campaign group: weight of callingList CL_Test_2 should be between 0 to 100"
        self.string_utils.assert_message_from_response(r['response'], expected_message=message)

    def test_130_edit_campaign_group_with_incorrect_lists_weighting_value(self):

        templ_id = self.campaign_utils.create_campaign_template(
            name="edit_campaign_group_with_incorrect_lists_weighting",
            dialing_profile_name="DialingProfilePreview")

        response = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                             callingLists=[{'name': 'CL_Test_2', 'weight': 100}],
                                                             campaignTemplateId=templ_id, dialMode='Preview')

        r = self.campaign_utils.edit_campaign_group_by_id(dbid=response["DBID"], groupName="AG_Test_1",
                                                          expected_code=400,
                                                          callingLists=[{'name': 'CL_Test_2', 'weight': 200}],
                                                          campaignTemplateId=templ_id)

        message = "Failed to update campaign by id: weight of callingList CL_Test_2 should be between 0 to 100"
        self.string_utils.assert_message_from_response(r, expected_message=message)

    def test_131_create_campaign_group_with_string_lists_weight(self):

        templ_id = self.campaign_utils.create_campaign_template(
            name="test_create_campaign_group_with_string_list_weight",
            dialing_profile_name="DialingProfilePreview")

        response = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                             callingLists=[{'name': 'CL_Test_2', 'weight': 'str'}],
                                                             campaignTemplateId=templ_id, dialMode='Preview',
                                                             expected_code=400)

        message = "Failed to create campaign group: weight of callingList CL_Test_2 should be numeric"
        self.string_utils.assert_message_from_response(response['response'], expected_message=message)

    def test_132_create_campaign_group_with_0_lists_weight(self):
        templ_id = self.campaign_utils.create_campaign_template(name="test_create_campaign_group_with_0_list_weight",
                                                                dialing_profile_name="DialingProfilePreview")
        list_weighting = 0

        name = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': list_weighting}],
                                                         campaignTemplateId=templ_id, dialMode='Preview',
                                                         expected_code=200)

        response = self.lists_utils.conf_server.return_object_from_cme_by_name(name=name["name"][0],
                                                                               object_type="CFGCampaign")
        obj = response.__dict__["callingLists"][0].__dict__["share"]

        assert obj == list_weighting, \
            "Incorrect property of callingLists is stored in CME, expected value is" \
            " 'share':{0} but actual value is 'share':{1} ".format(list_weighting, obj["share"])

    def test_133_create_campaign_group_with_incorrect_second_list_weighting(self):
        templ_id = self.campaign_utils.create_campaign_template(name="test_create_campaign_group_with_incorrect_list",
                                                                dialing_profile_name="DialingProfilePreview")
        response = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                             callingLists=[{'name': 'CL_Test_2', 'weight': 100},
                                                                           {'name': 'CL_Test_1', 'weight': 101}],
                                                             campaignTemplateId=templ_id, dialMode='Preview',
                                                             expected_code=400)

        message = "Failed to create campaign group: weight of callingList CL_Test_1 should be between 0 to 100"
        self.string_utils.assert_message_from_response(response['response'], expected_message=message)

    def test_134_edit_campaign_group_to_incorrect_second_list_weighting(self):

        templ_id = self.campaign_utils.create_campaign_template(
            name="test_edit_campaign_group_to_incorrect_second_list",
            dialing_profile_name="DialingProfilePreview")

        response = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                             callingLists=[{'name': 'CL_Test_2', 'weight': 99},
                                                                           {'name': 'CL_Test_1', 'weight': 100}],
                                                             campaignTemplateId=templ_id, dialMode='Preview',
                                                             expected_code=200)

        r = self.campaign_utils.edit_campaign_group_by_id(dbid=response["DBID"], groupName="AG_Test_1",
                                                          expected_code=400,
                                                          callingLists=[{'name': 'CL_Test_2', 'weight': 201}],
                                                          campaignTemplateId=templ_id)

        message = "Failed to update campaign by id: weight of callingList CL_Test_2 should be between 0 to 100"
        self.string_utils.assert_message_from_response(r, expected_message=message)

    def test_135_create_campaign_group_with_two_valid_list_weights(self):
        list_1 = self.lists_utils.post_list(upload_file=os.path.abspath('api_aggregator/files/CL_DNC.txt'),
                                            name="CL_test_list_1", spec_id=self.spec_id)
        list_2 = self.lists_utils.post_list(upload_file=os.path.abspath('api_aggregator/files/CL_DNC.txt'),
                                            name="CL_test_list_2", spec_id=self.spec_id)
        templ_id = self.campaign_utils.create_campaign_template(name="create_campaign_group_with_two_valid_list",
                                                                dialing_profile_name="DialingProfilePreview")
        name = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_test_list_1', 'weight': 0},
                                                                       {'name': 'CL_test_list_2', 'weight': 1}],
                                                         campaignTemplateId=templ_id, dialMode='Preview',
                                                         expected_code=200)
        response = self.lists_utils.conf_server.return_object_from_cme_by_name(name=name["name"][0],
                                                                               object_type="CFGCampaign")
        self.campaign_utils.check_list_weighting_in_campaign(obj=response, list_id=list_1, expected_share=0)
        self.campaign_utils.check_list_weighting_in_campaign(obj=response, list_id=list_2, expected_share=1)

        # self.campaign_utils.check_list_weighting_in_campaign(obj=response, list_id=[list_1, list_2],
        #                                                      expected_share=[0, 1]) TO_DO: wait for method updating

    def test_136_get_campaign_group_statistics_list_size(self):
        list_file = self.lists_utils.file_utils.make_file(list_type='calling list', scdfrom=False, till=False,
                                                          name="cl_campaign1",
                                                          extension='txt', records_count=randint(20, 50))
        self.lists_utils.post_list(upload_file=list_file, name="Call_list_statistic_size", spec_id=self.spec_id)
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ,
                                                         dialMode='Progress',
                                                         callingLists=[{'name': 'Call_list_statistic_size',
                                                                        'weight': 1}])
        response = self.campaign_utils.get_campaign_groups_statistics(campaign_group_dbid=resp["DBID"])["data"]
        try:
            self.cm_utils.check_campaign_group_list_size(calling_lists="Call_list_statistic_size", statistics=response)
        except Exception as e:
            raise Exception("Failed to check List Size statistic of "
                            "campaign group {0} with exception: {1}".format(resp["name"][0] + resp["name"][1], e))

    def test_137_get_campaign_group_statistics_list_size_with_several_call_lists(self):
        call_lists = []
        list_file = self.lists_utils.file_utils.make_file(list_type='calling list', name="cl_campaign2",
                                                          extension='txt',
                                                          records_count=10)
        for num in range(3):
            name = "Call_list_statistic_size_" + str(num)
            self.lists_utils.post_list(upload_file=list_file, name=name, spec_id=self.spec_id)
            call_lists.append({"name": name, "weight": 1})
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ,
                                                         dialMode='Progress', callingLists=call_lists)
        time.sleep(3)
        response = self.campaign_utils.get_campaign_groups_statistics(campaign_group_dbid=resp["DBID"])["data"]
        try:
            call_lists = [lists["name"] for lists in call_lists]
            self.cm_utils.check_campaign_group_list_size(calling_lists=call_lists, statistics=response)
        except Exception as e:
            raise Exception("Failed to check List Size statistic of "
                            "campaign group '{0}' with exception: {1}".format(resp["name"][0] + resp["name"][1], e))

    def test_138_check_campaign_group_statistics_list_size_after_deletion_second_call_list(self):
        call_lists = []
        file_name = self.string_utils.rand_string(8)
        list_file = self.lists_utils.file_utils.make_file(list_type='calling list', name=file_name, extension='txt',
                                                          records_count=50)
        for num in range(2):
            name = "Call_list_statistic_size_deletion" + str(num)
            self.lists_utils.post_list(upload_file=list_file, name=name, spec_id=self.spec_id)
            call_lists.append({"name": name, "weight": 1})

        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_2', campaignTemplateId=self.templ_id,
                                                         dialMode='Progress', callingLists=call_lists)
        del call_lists[0]
        self.campaign_utils.edit_campaign_group_by_id(dbid=resp["DBID"], groupName='AG_Test_2',
                                                      campaignTemplateId=self.templ_id,
                                                      dialMode='Progress', callingLists=call_lists, expected_code=200)
        time.sleep(3)
        lists_stat = self.campaign_utils.get_campaign_groups_statistics(campaign_group_dbid=resp["DBID"])["data"]
        try:
            call_lists = [lists["name"] for lists in call_lists]
            self.cm_utils.check_campaign_group_list_size(calling_lists=call_lists, statistics=lists_stat)
        except Exception as e:
            raise Exception("Failed to check List Size statistic of "
                            "campaign group '{0}' with exception: {1}".format(resp["name"][0] + resp["name"][1], e))

    # def test_133_get_running_campaign_group_statistics_list_delivered(self):
    #     list_file = self.lists_utils.file_utils.make_file(list_type='calling list', scdfrom=False, till=False, name="cl_campaign3",
    #                                            extension='txt', records_count=10)
    #     self.lists_utils.post_list(upload_file=list_file, name="Call_list_statistic_delivered", spec_id=self.spec_id)
    #     resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ,
    #                                                      dialMode='Preview', start=True,
    #                                                      callingLists=[{'name': 'Call_list_statistic_delivered',
    #                                                                     'weight': 1}])
    #     sleep(3)
    #     response = self.cm_utils.get_campaign_group_statistics(campaign_group_dbid=resp["DBID"],
    #                                                            token=False)["data"]
    #     try:
    #         self.cm_utils.check_campaign_group_list_delivered(calling_lists="Call_list_statistic_delivered",
    #                                                           statistics=response, state="Active")
    #     except Exception as e:
    #         raise Exception("Failed to check List Delivered statistic of Running"
    #                         "campaign group '{0}' with exception: {1}".format(resp["name"][0] + resp["name"][1], e))
    #
    # def test_134_get_active_campaign_group_statistics_list_delivered(self):
    #     list_file = self.lists_utils.file_utils.make_file(list_type='calling list', scdfrom=False, till=False,
    #                                            name="cl_campaign4",
    #                                            extension='txt', records_count=10)
    #     self.lists_utils.post_list(upload_file=list_file, name="Call_list_statistic_delivered1",
    #                                spec_id=self.spec_id)
    #     resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ,
    #                                                      dialMode='Preview', start=True,
    #                                                      callingLists=[{'name': 'Call_list_statistic_delivered1',
    #                                                                     'weight': 1}])
    #     self.campaign_utils.campaign_start_pause_stop_resume(action="pause", campaign_name=resp["name"][0],
    #                                                          group_name=resp["name"][1])
    #     sleep(3)
    #     response = self.cm_utils.get_campaign_group_statistics(campaign_group_dbid=resp["DBID"],
    #                                                            token=False)["data"]
    #     try:
    #         self.cm_utils.check_campaign_group_list_delivered(calling_lists="Call_list_statistic_delivered1",
    #                                                           statistics=response, state="Paused")
    #     except Exception as e:
    #         raise Exception("Failed to check List Delivered statistic of Active"
    #                         "campaign group '{0}' with exception: {1}".format(resp["name"][0] + resp["name"][1], e))
    #
    # def test_135_get_not_loaded_campaign_group_statistics_list_delivered(self):
    #     list_file = self.lists_utils.file_utils.make_file(list_type='calling list', scdfrom=False, till=False,
    #                                            name="cl_campaign4",
    #                                            extension='txt', records_count=10)
    #     self.lists_utils.post_list(upload_file=list_file, name="Call_list_statistic_delivered2",
    #                                spec_id=self.spec_id)
    #     resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ,
    #                                                      dialMode='Preview', start=True,
    #                                                      callingLists=[{'name': 'Call_list_statistic_delivered2',
    #                                                                     'weight': 1}])
    #     self.campaign_utils.campaign_start_pause_stop_resume(action="stop", campaign_name=resp["name"][0],
    #                                                          group_name=resp["name"][1])
    #     sleep(3)
    #     response = self.cm_utils.get_campaign_group_statistics(campaign_group_dbid=resp["DBID"],
    #                                                            token=False)["data"]
    #     try:
    #         self.cm_utils.check_campaign_group_list_delivered(calling_lists="Call_list_statistic_delivered2",
    #                                                           statistics=response, state="Frozen")
    #     except Exception as e:
    #         raise Exception("Failed to check List Delivered statistic of Stopped"
    #                         "campaign group '{0}' with exception: {1}".format(resp["name"][0] + resp["name"][1], e))

    def test_136_check_preload_with_location_rule_anywhere(self):
        rule = self.compliance_utils.post_location_rule(name="Location_rule_for_preload", locationBy="device",
                                                        priority=1, locationType="anywhere")
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": 'Location_preload', "weight": 10}],
                                                         campaignTemplateId=self.templ_id,
                                                         locationRules=[rule["payload"]["data"]["name"]],
                                                         start=True)
        self.campaign_utils.check_preload_campaign(calling_lists=[self.preload_list_id], camp_group_dbid=resp["DBID"],
                                                   reason=23)

    def test_137_check_preload_with_location_rule_regions_type_all_regions(self):
        rule = self.compliance_utils.post_location_rule(name="Location_rule_for_preload_ALL", locationBy="device",
                                                        priority=1, locationType="regions",
                                                        csvLocations=self.string_utils.locations,
                                                        wirelessRegions="", dncRegions="")
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": 'Location_preload', "weight": 10}],
                                                         campaignTemplateId=self.templ_id,
                                                         locationRules=[rule["payload"]["data"]["name"]], start=True)
        self.campaign_utils.check_preload_campaign(calling_lists=[self.preload_list_id],
                                                   camp_group_dbid=resp["DBID"], reason=19)

    def test_138_check_preload_with_location_rule_time_zone(self):
        rule = self.compliance_utils.post_location_rule(name="Location_rule_for_preload_TZ", locationBy="device",
                                                        priority=1, locationType="timeZones",
                                                        csvLocations=["America/New_York"])
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": 'Location_preload', "weight": 10}],
                                                         campaignTemplateId=self.templ_id,
                                                         locationRules=[rule["payload"]["data"]["name"]], start=True)
        self.campaign_utils.check_preload_campaign(calling_lists=[self.preload_list_id], camp_group_dbid=resp["DBID"],
                                                   reason=21)

    def test_139_check_preload_with_location_rule_area_code(self):
        rule = self.compliance_utils.post_location_rule(name="Location_rule_for_preload_AC", locationBy="device",
                                                        priority=2, locationType="areaCodes", csvLocations="213")
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": 'Location_preload', "weight": 10}],
                                                         campaignTemplateId=self.templ_id,
                                                         locationRules=[rule["payload"]["data"]["name"]], start=True)
        self.campaign_utils.check_preload_campaign(calling_lists=[self.preload_list_id], camp_group_dbid=resp["DBID"],
                                                   reason=22)

    def test_140_check_preload_with_location_rule_country_code(self):
        rule = self.compliance_utils.post_location_rule(name="Location_rule_for_preload_CC", locationBy="clientid",
                                                        priority=2, locationType="countryCodes", csvLocations=["US"])
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": 'Location_preload', "weight": 10}],
                                                         campaignTemplateId=self.templ_id,
                                                         locationRules=[rule["payload"]["data"]["name"]], start=True)
        self.campaign_utils.check_preload_campaign(calling_lists=[self.preload_list_id], camp_group_dbid=resp["DBID"],
                                                   reason=20)

    def test_141_start_campaign_group_with_location_rule_postalCodes_and_check_preload(self):
        name = "test_159_check_keywords_in_spec_file_{0}".format(str("postal_code").replace(" ", "-"))
        field_list = "FirstName,LastName,Company,Device1,Device2,Device3,Device4,Device5,Device6,Device7,Device8," \
                     "Device9,Device10,Other1,Other2,Other3,Other4,Other5,Other6,Other7,Other8,Other9,Other10," \
                     "Other11,Other12,Other13,Other14,Other15,Other16,Other17,Other18,Other19,Other20,{0}," \
                     "ClientID".format("postal_code")
        key_spec = self.lists_utils.file_utils.make_advanced_spec_file(name="{0}_spec".format(name),
                                                                       field_list=list(field_list.split(",")))
        spec_id = self.lists_utils.post_specifications(name="{}_spec".format(name),
                                                       upload_file=os.path.abspath(key_spec))
        up_file = os.path.abspath("campaign_manager/files/test_159_check_keywords_in_spec_file_postal_code.csv")
        list_id = self.lists_utils.post_list(name=name, upload_file=up_file, spec_id=spec_id,
                                             check_in_db=False)
        records = self.db_utils.get_records_from_db_with_parameters(table_name='cc_list_' + str(list_id),
                                                                    column_names="c_postal_code")
        rule = self.compliance_utils.post_location_rule(name="Location_rule_for_preload_postal_codes",
                                                        locationBy="clientid", priority=1, locationType="postalCodes",
                                                        csvLocations=choice(records))
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": name, "weight": 10}],
                                                         campaignTemplateId=self.templ_id,
                                                         locationRules=[rule["payload"]["data"]["name"]], start=True)
        self.campaign_utils.check_preload_campaign(calling_lists=[self.preload_list_id], camp_group_dbid=resp["DBID"],
                                                   reason=24)

    def test_142_campaign_group_with_required_rules(self):
        camp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ_id,
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}])
        info = self.compliance_utils.post_location_rule(name='LocationRule_142', required=True)
        info1 = self.compliance_utils.post_attempt_rule(name='AttemptRule_142', required=True)
        supp_list = self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='Supp_list_142',
                                                           suppression_type='deviceIndex', expiration_date='-1',
                                                           check_in_db=False, required='true', return_id=True)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(name=camp['name'][0] + "@" + camp['name'][1],
                                                                          object_type="CFGCampaignGroup")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter='locationRules',
                                                                    value=["LocationRule_142"])
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter="attemptRules",
                                                                    value=["AttemptRule_142"])
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter="suppressionLists",
                                                                    value=["Supp_list_142"])
        self.compliance_utils.delete_compliance_rule(rule_id=info['ID'], rule_type='location')
        self.compliance_utils.delete_compliance_rule(rule_id=info1['ID'])
        self.lists_utils.delete_suppression_list(supp_list_id=supp_list)

    def test_143_create_campaign_group_with_agent_group_without_dn(self):
        self.lists_utils.conf_server.create_cfg_agent_group("Agent group without DN")
        self.campaign_utils.create_campaign_group(groupName='Agent group without DN',
                                                  campaignTemplateId=self.templ_id,
                                                  callingLists=[{'name': 'CL_Test_2', 'weight': 1}])

    def test_144_create_campaign_group_with_number_of_ports_from_session_profile(self):
        session_profile_name = "session_profile_max_size"
        max_size = randint(2, 999)
        self.campaign_utils.create_session_profile(profile_name=session_profile_name, max_queue_size=max_size)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(name=session_profile_name,
                                                                          object_type="CFGScript")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter="sessionProfile",
                                                                    value=['"maxQueueSize":{}'.format(max_size)])
        templ_id = self.campaign_utils.create_campaign_template(name="Templ_maxQueueSize",
                                                                session_profile_name=session_profile_name,
                                                                dialing_profile_name="DialingProfilePreview")
        obj_2 = self.lists_utils.conf_server.return_object_from_cme_by_name(name="Templ_maxQueueSize",
                                                                            object_type="CFGScript")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj_2, parameter="campaignTemplate",
                                                                    value=['"maxQueueSize":{}'.format(max_size)])
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                         campaignTemplateId=templ_id, dialMode='Preview')
        obj_3 = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=resp["name"][0] + "@" + resp["name"][1],
            object_type="CFGCampaignGroup")
        max_size_cme = obj_3.__dict__["maxQueueSize"]
        assert max_size == max_size_cme, "Expected value of maxQueueSize in - {0} is not equal to actual value" \
                                         " in CME - {1}".format(max_size, max_size_cme)

    def test_145_create_campaign_group_using_device_escalation_and_answering_machine_true(self):
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   campaignTemplateId=self.templ_predictive,
                                                                   callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                                   useDeviceEscalation=True, delivery={"AM": "3501"},
                                                                   start=True)

        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=obj, use_device_escalation=True)

    def test_146_get_campaign_group_without_read_access(self):
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   campaignTemplateId=self.templ_predictive,
                                                                   callingLists=[{'name': 'CL_Test_2', 'weight': 10}])
        self.api.get_object_without_read_access(required_object="campaign/get-campaign-group/" +
                                                                str(campaign_group["DBID"]))

    def test_147_create_campaign_group_with_device_escalation_and_order_by(self):
        filtering_rule = self.lists_utils.post_filtering_rule(name=self.string_utils.rand_string(7) + "_rule",
                                                              ascDesc=[{"field": "firstname", "sort": "asc"}])
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   callingLists=[{'name': 'CL_Test_0', 'weight': 10}],
                                                                   campaignTemplateId=self.templ_id,
                                                                   filteringRuleId=filtering_rule["DBID"],
                                                                   useDeviceEscalation=True, start=True)
        order_by = "firstname asc"
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=obj, use_device_escalation=True,
                                                                               order_by=order_by)

    def test_148_create_campaign_group_with_change_device_priority_in_device_escalation(self):
        case = "chain_id asc, (case when cd_device_index = 2 then 1 when cd_device_index = 3 then 2 when" \
               " cd_device_index = 1 then 3 end)"
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   campaignTemplateId=self.templ_predictive,
                                                                   callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                                   useDeviceEscalation=True, delivery={"AM": "3501"},
                                                                   Device1=2, Device2=3, Device3=1,
                                                                   useDeviceEscalation4=False,
                                                                   useDeviceEscalation5=False,
                                                                   useDeviceEscalation6=False,
                                                                   useDeviceEscalation7=False,
                                                                   useDeviceEscalation8=False,
                                                                   useDeviceEscalation9=False,
                                                                   useDeviceEscalation10=False, start=True)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        escalation = json.loads(obj.__dict__['userProperties']['CloudContact']['escalation'])
        assert escalation['order_by'] == case, "Expected value {0} of order_by field in CloudContact section is not" \
                                               " equal to actual value in CME {1}".format(case, escalation['order_by'])
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=obj, use_device_escalation=True,
                                                                               case=case)

    def test_149_edit_campaign_group_by_change_device_priority_in_device_escalations(self):
        case = "chain_id asc, (case when cd_device_index = 2 then 1 when cd_device_index = 3 then 2 when" \
               " cd_device_index = 1 then 3 end)"
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   campaignTemplateId=self.templ_predictive,
                                                                   callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                                   useDeviceEscalation=True, delivery={"AM": "3501"},
                                                                   Device1=2, Device2=3, Device3=1, start=True,
                                                                   useDeviceEscalation4=False,
                                                                   useDeviceEscalation5=False,
                                                                   useDeviceEscalation6=False,
                                                                   useDeviceEscalation7=False,
                                                                   useDeviceEscalation8=False,
                                                                   useDeviceEscalation9=False,
                                                                   useDeviceEscalation10=False)
        time.sleep(10)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        escalation = json.loads(obj.__dict__['userProperties']['CloudContact']['escalation'])
        assert escalation['order_by'] == case, "Expected value {0} of order_by field in CloudContact section is not" \
                                               " equal to actual value in CME {1}".format(case, escalation['order_by'])
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=obj, use_device_escalation=True,
                                                                               case=case)

        case_after_edit = "chain_id asc, (case when cd_device_index = 10 then 1 when cd_device_index = 9 then 2 when cd" \
                          "_device_index = 7 then 3 end)"
        self.campaign_utils.campaign_start_pause_stop_resume(action="stop", campaign_name=campaign_group['name'][0],
                                                             group_name=campaign_group['name'][1])
        time.sleep(5)
        self.campaign_utils.edit_campaign_group_by_id(dbid=campaign_group["DBID"], groupName='AG_Test_1',
                                                      campaignTemplateId=self.templ_predictive,
                                                      callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                      useDeviceEscalation=True, delivery={"AM": "3501"},
                                                      Device1=10, Device2=9, Device3=7, useDeviceEscalation4=False,
                                                      useDeviceEscalation5=False, useDeviceEscalation6=False,
                                                      useDeviceEscalation10=False, useDeviceEscalation8=False,
                                                      useDeviceEscalation7=False, useDeviceEscalation9=False, start=True)
        edit_obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        edit_escalation = json.loads(edit_obj.__dict__['userProperties']['CloudContact']['escalation'])
        assert edit_escalation['order_by'] == case_after_edit, \
            "Expected value {0} of order_by field in CloudContact section is not equal to actual value in CME" \
            " {1}".format(case, escalation['order_by'])
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=edit_obj, use_device_escalation=True,
                                                                               case=case_after_edit)

    def test_150_create_campaign_group_with_will_be_dropped_devices(self):
        am_drop_field = "cd_device_index:1,2,3"
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   campaignTemplateId=self.templ_predictive,
                                                                   callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                                   useDeviceEscalation=True, isConnect1=False,
                                                                   isConnect2=False, isConnect3=False,
                                                                   delivery={"AM": "3501"}, start=True)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        escalation = json.loads(obj.__dict__['userProperties']['CloudContact']['escalation'])
        assert escalation['am-drop-field'] == am_drop_field, \
            "Expected value {0} of am_drop_field field in CloudContact section is not equal to actual value in CME" \
            " {1}".format(am_drop_field, escalation['am-drop-field'])
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=obj, use_device_escalation=True,
                                                                               am_drop_field=True,
                                                                               cd_device_index="1,2,3")

    def test_151_create_campaign_group_with_all_will_be_dropped_devices(self):
        am_drop_field = "cd_device_index:1,2,3,4,5,6,7,8,9,10"
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   campaignTemplateId=self.templ_predictive,
                                                                   callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                                   useDeviceEscalation=True, start=True)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        escalation = json.loads(obj.__dict__['userProperties']['CloudContact']['escalation'])
        assert escalation['am-drop-field'] == am_drop_field, \
            "Expected value {0} of am_drop_field field in CloudContact section is not equal to actual value in CME" \
            " {1}".format(am_drop_field, escalation['am-drop-field'])
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=obj, use_device_escalation=True,
                                                                               am_drop_field=True)

    def test_152_edit_campaign_group_by_change_will_be_connected_devices_to_will_be_dropped(self):
        am_drop_field = "cd_device_index:1,2,3"
        campaign_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                                   campaignTemplateId=self.templ_predictive,
                                                                   callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                                   useDeviceEscalation=True, delivery={"AM": "3501"})

        self.campaign_utils.edit_campaign_group_by_id(groupName='AG_Test_1', campaignTemplateId=self.templ_predictive,
                                                      callingLists=[{'name': 'CL_Test_2', 'weight': 10}],
                                                      useDeviceEscalation=True, isConnect1=False, isConnect2=False,
                                                      isConnect3=False, delivery={"AM": "3501"},
                                                      dbid=campaign_group["DBID"], start=True)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(
            name=campaign_group["name"][0] + "@" + campaign_group["name"][1], object_type='CFGCampaignGroup')
        escalation = json.loads(obj.__dict__['userProperties']['CloudContact']['escalation'])
        assert escalation['am-drop-field'] == am_drop_field, \
            "Expected value {0} of am_drop_field field in CloudContact section is not equal to actual value in CME" \
            " {1}".format(am_drop_field, escalation['am-drop-field'])
        self.lists_utils.conf_server.check_device_escalation_in_campaign_group(obj=obj, use_device_escalation=True,
                                                                               am_drop_field=True,
                                                                               cd_device_index="1,2,3")

    def test_152_create_campaign_group_with_retry_options_disposition_code(self):
        dc_name = self.string_utils.rand_string(8)
        self.lists_utils.conf_server.create_disposition_code_in_cme(dc_name=dc_name)
        self.campaign_utils.get_disposition_codes()

    def test_153_edit_rules_to_required_and_check_changes_in_campaign_group(self):
        camp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', campaignTemplateId=self.templ_id,
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}])
        info = self.compliance_utils.post_location_rule(name='LocationRule_143')
        info1 = self.compliance_utils.post_attempt_rule(name='AttemptRule_143')
        supp_list = self.lists_utils.post_suppression_list(upload_file=self.supp_file, name='Supp_list_143',
                                                           suppression_type='deviceIndex', expiration_date='3',
                                                           check_in_db=False, return_id=True)
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(name=camp['name'][0] + "@" + camp['name'][1],
                                                                          object_type="CFGCampaignGroup")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter='locationRules',
                                                                    value=[""])
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter="attemptRules",
                                                                    value=[""])
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj, parameter="suppressionLists",
                                                                    value=[""])
        self.compliance_utils.put_location_rule(location_id=info['ID'], payload=info['payload'], required=True)
        self.compliance_utils.put_attempt_rule(attempt_id=info1['ID'], payload=info1['payload'], required=True)
        self.lists_utils.put_suppression_list(name="Supp_list_143", supp_list_id=supp_list, required=True)
        obj_after = self.lists_utils.conf_server.return_object_from_cme_by_name(name=camp['name'][0] + "@" + camp['name'][1],
                                                                                object_type="CFGCampaignGroup")
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj_after, parameter='locationRules',
                                                                    value=["LocationRule_143"])
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj_after, parameter="attemptRules",
                                                                    value=["AttemptRule_143"])
        self.lists_utils.conf_server.get_object_property_from_annex(object_from_cme=obj_after, parameter="suppressionLists",
                                                                    value=["Supp_list_143"])
        self.compliance_utils.delete_compliance_rule(rule_id=info['ID'], rule_type='location')
        self.compliance_utils.delete_compliance_rule(rule_id=info1['ID'])
        self.lists_utils.delete_suppression_list(supp_list_id=supp_list)

    def test_154_add_campaign_group_with_arbitrary_options(self):
        arbitrary_options = [{"name": "TestArbitrary1", "value": "yes"},
                             {"name": "TestArbitrary2", "value": "yes"}]
        self.campaign_utils.create_dialing_profile(name='ArbitraryDialingProfile', groupName='AG_Test_1')
        template = self.campaign_utils.create_campaign_template(name='ArbitraryCampaignTemplate',
                                                                dialing_profile_name='ArbitraryDialingProfile')
        camp_group = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', dialMode='Predict',
                                                               callingLists=[{'name': 'CL_Test_1', 'weight': 1}],
                                                               campaignTemplateId=template,
                                                               arbitrary=arbitrary_options)
        cg_name = camp_group['response'].json()['data']['name']
        obj = self.lists_utils.conf_server.return_object_from_cme_by_name(name=cg_name, object_type='CFGCampaignGroup')
        arbitrary_from_cloudcontact = json.loads(obj.userProperties['CloudContact']['arbitrary'])
        assert arbitrary_from_cloudcontact == arbitrary_options, \
            "Expected advanced options {0} do not match with actual {1}".format \
                (arbitrary_options, arbitrary_from_cloudcontact)
        ocs_data = obj.userProperties['OCServer']
        for option in arbitrary_options:
            if option["name"] not in ocs_data:
                raise Exception()
            else:
                assert ocs_data[option["name"]] == option["value"], \
                    "Expected advanced options {0} do not match with actual {1}".format \
                        (arbitrary_options, ocs_data)

    def test_155_negative_create_campaign_group_with_invalid_optMethodValue_progress_dial_mode(self):
        opt_method = randint(1, 100)
        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', ivrMode=True,
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         campaignTemplateId=self.ivr_templ, dialMode='Progress',
                                                         optMethod="OverdialRate", optMethodValue=opt_method,
                                                         expected_code=400)
        message = 'Failed to create campaign group: optMethodValue always should be 0'
        self.string_utils.assert_message_from_response(resp['response'], expected_message=message)

    def test_156_negative_update_campaign_group_with_invalid_optMethodValue_progress_dial_mode(self):
        opt_method = randint(1, 100)
        dbid = self.campaign_utils.create_campaign_group(groupName='AG_Test_1', ivrMode=True,
                                                         callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                         campaignTemplateId=self.ivr_templ, dialMode='Progress',
                                                         optMethod="OverdialRate", optMethodValue=0)
        resp = self.campaign_utils.edit_campaign_group_by_id(dbid=dbid['DBID'], groupName="AG_Test_1",
                                                             campaignTemplateId=self.ivr_templ, dialMode='Progress',
                                                             callingLists=[{'name': 'CL_Test_2', 'weight': 1}],
                                                             optMethodValue=opt_method, expected_code=400, ivrMode=True,
                                                             optMethod="OverdialRate")
        message = 'Failed to update campaign by id: optMethodValue always should be 0'
        self.string_utils.assert_message_from_response(resp, expected_message=message)

    def test_157_check_preload_campaign_with_till_from_value_in_4_of_5_contact_of_required_supp_list(self):
        supp_file = os.path.abspath("campaign_manager/files/check_preload_till_from.csv")
        sup_id = self.lists_utils.post_suppression_list(name="SupForPreloadTillFrom", upload_file=supp_file,
                                                        suppression_type='deviceIndex',
                                                        expiration_date=-1, return_id=True, spec_id=self.spec_id,
                                                        required='true')
        call_list_file = os.path.abspath("campaign_manager/files/check_preload_till_from.csv")
        list_id = self.lists_utils.post_list(upload_file=call_list_file, name='CL_Till_From')

        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": 'CL_Till_From', "weight": 10}],
                                                         campaignTemplateId=self.templ_id)
        self.campaign_utils.campaign_start_pause_stop_resume(action='start', campaign_name=resp["name"][0],
                                                             group_name=resp["name"][1])
        # Reason 1 means mandatory suppression list
        filtered_count = self.campaign_utils.check_preload_campaign(calling_lists=[list_id], suppression_lists=[sup_id],
                                                                    camp_group_dbid=resp["DBID"], reason=1)
        # One device should be filtered during preload because this device doesn't have till from value
        assert len(filtered_count) == 1, "Expected count of filtered devices should be 1"
        self.lists_utils.delete_suppression_list(sup_id)

    def test_158_check_preload_campaign_with_till_from_value_in_all_contact_of_optional_supp_list(self):
        supp_file = os.path.abspath("campaign_manager/files/check_preload_till_from.csv")
        sup_id = self.lists_utils.post_suppression_list(name="SupForPreloadTillFrom_optional", upload_file=supp_file,
                                                        suppression_type='deviceIndex',
                                                        expiration_date=-1, return_id=True, spec_id=self.spec_id)
        call_list_file = os.path.abspath("campaign_manager/files/check_preload_till_from.csv")
        list_id = self.lists_utils.post_list(upload_file=call_list_file, name='CL_Till_From_optional')

        resp = self.campaign_utils.create_campaign_group(groupName='AG_Test_1',
                                                         callingLists=[{"name": 'CL_Till_From_optional', "weight": 10}],
                                                         campaignTemplateId=self.templ_id,
                                                         suppressionLists="SupForPreloadTillFrom_optional")
        self.campaign_utils.campaign_start_pause_stop_resume(action='start', campaign_name=resp["name"][0],
                                                             group_name=resp["name"][1])
        # Reason 2 means optional suppression list
        filtered_count = self.campaign_utils.check_preload_campaign(calling_lists=[list_id], suppression_lists=[sup_id],
                                                                    camp_group_dbid=resp["DBID"], reason=2)
        # One device should be filtered during preload because this device doesn't have till from value
        assert len(filtered_count) == 1, "Expected count of filtered devices should be 1"
