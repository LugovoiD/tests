import csv
import re
from random import choice, randint
import phonenumbers
from faker import Faker
from phonenumbers import timezone
from api_utils.utils import Singleton


class DeviceUtils:

    __metaclass__ = Singleton

    def __init__(self):
        self.fake = Faker('en_US')

    def generate_number(self, unformated=False):

        # About US phone numbers can be read here:
        # https://en.wikipedia.org/wiki/North_American_Numbering_Plan#Modern_plan
        second = randint(0, 9)
        third = randint(0, 9) if second != 1 else randint(2, 9)
        exchange = '{}{}{}'.format(randint(2, 9), second, third)
        number = "+1{0}{1}{2}".format(self.get_area_code(), exchange, randint(1000, 9999))
        check = phonenumbers.parse(number, "US")
        tz = str(timezone.time_zones_for_number(check))
        if not phonenumbers.is_valid_number(check) or isinstance(tz, unicode) or len(tz) > 40:
            correct_number = self.generate_number()
        else:
            modes = [lambda: str(phonenumbers.format_number(check, phonenumbers.PhoneNumberFormat.INTERNATIONAL)),
                     lambda: str(phonenumbers.format_number(check, phonenumbers.PhoneNumberFormat.NATIONAL)),
                     lambda: str(phonenumbers.format_number(check, phonenumbers.PhoneNumberFormat.E164))]
            correct_number = choice(modes)()
        if unformated:
            return correct_number, number
        else:
            return correct_number

    def rand_device(self, can_be_none=True, correct=False, gb_number=False, used_devices=None):
        device = None
        unformated_device = None
        gb = lambda: self.normalize_device(Faker('en_GB').phone_number(), country='GB')
        if can_be_none:
            number = [gb, self.fake.email, '', ''] if gb_number else [self.generate_number, self.fake.email, '', '']
            if not correct:
                number.append('')
            number = choice(number)
        else:
            number = gb if gb_number else self.generate_number
        while (device is None) or (False if not unformated_device else (unformated_device in used_devices)):
            if used_devices is not None:
                if callable(number) and 'generate_number' in number.__name__:
                    device, unformated_device = number(unformated=True)
                else:
                    device = unformated_device = number() if callable(number) else number
            else:
                device = number() if callable(number) else number
        if unformated_device:
            used_devices.append(unformated_device)
        return device

    @staticmethod
    def get_area_code(area_code=None):
        area_codes = {"201": "NJ", "202": "DC", "203": "CT", "204": "MB", "205": "AL", "206": "WA", "207": "ME",
                      "208": "ID", "209": "CA", "210": "TX", "212": "NY", "213": "CA", "214": "TX", "215": "PA",
                      "216": "OH", "217": "IL", "218": "MN", "219": "IN", "224": "IL", "225": "LA", "226": "ON",
                      "228": "MS", "229": "GA", "231": "MI", "234": "OH", "236": "BC", "239": "FL", "240": "MD",
                      "248": "MI", "250": "BC", "251": "AL", "252": "NC", "253": "WA", "254": "TX", "256": "AL",
                      "260": "IN", "262": "WI", "267": "PA", "269": "MI", "270": "KY", "276": "VA", "278": "MI",
                      "281": "TX", "283": "OH", "289": "ON", "301": "MD", "302": "DE", "303": "CO", "304": "WV",
                      "305": "FL", "306": "SK", "307": "WY", "308": "NE", "309": "IL", "310": "CA", "312": "IL",
                      "313": "MI", "314": "MO", "315": "NY", "316": "KS", "317": "IN", "318": "LA", "319": "IA",
                      "320": "MN", "321": "FL", "323": "CA", "325": "TX", "330": "OH", "331": "IL", "334": "AL",
                      "336": "NC", "337": "LA", "339": "MA", "340": "VI", "341": "CA", "343": "ON", "347": "NY",
                      "351": "MA", "352": "FL", "360": "WA", "361": "TX", "365": "ON", "369": "CA", "380": "OH",
                      "385": "UT", "386": "FL", "401": "RI", "402": "NE", "403": "AB", "404": "GA", "405": "OK",
                      "406": "MT", "407": "FL", "408": "CA", "409": "TX", "410": "MD", "412": "PA", "413": "MA",
                      "414": "WI", "415": "CA", "416": "ON", "417": "MO", "418": "QC", "419": "OH", "423": "TN",
                      "424": "CA", "425": "WA", "430": "TX", "431": "MB", "432": "TX", "434": "VA", "435": "UT",
                      "437": "ON", "438": "QC", "440": "OH", "442": "CA", "443": "MD", "450": "QC", "464": "IL",
                      "469": "TX", "470": "GA", "475": "CT", "478": "GA", "479": "AR", "480": "AZ", "481": "QC",
                      "484": "PA", "501": "AR", "502": "KY", "503": "OR", "504": "LA", "505": "NM", "506": "NB",
                      "507": "MN", "508": "MA", "509": "WA", "510": "CA", "512": "TX", "513": "OH", "514": "QC",
                      "515": "IA", "516": "NY", "517": "MI", "518": "NY", "519": "ON", "520": "AZ", "530": "CA",
                      "539": "OK", "540": "VA", "541": "OR", "548": "ON", "551": "NJ", "557": "MO", "559": "CA",
                      "561": "FL", "562": "CA", "563": "IA", "564": "WA", "567": "OH", "570": "PA", "571": "VA",
                      "573": "MO", "574": "IN", "575": "NM", "579": "QC", "580": "OK", "585": "NY", "586": "MI",
                      "587": "AB", "601": "MS", "602": "AZ", "603": "NH", "604": "BC", "605": "SD", "606": "KY",
                      "607": "NY", "608": "WI", "609": "NJ", "610": "PA", "612": "MN", "613": "ON", "614": "OH",
                      "615": "TN", "616": "MI", "617": "MA", "618": "IL", "619": "CA", "620": "KS", "623": "AZ",
                      "626": "CA", "627": "CA", "628": "CA", "630": "IL", "631": "NY", "636": "MO", "639": "SK",
                      "641": "IA", "646": "NY", "647": "ON", "650": "CA", "651": "MN", "657": "CA", "660": "MO",
                      "661": "CA", "662": "MS", "669": "CA", "670": "MP", "671": "GU", "678": "GA", "679": "MI",
                      "681": "WV", "682": "TX", "689": "FL", "701": "ND", "702": "NV", "703": "VA", "704": "NC",
                      "705": "ON", "706": "GA", "707": "CA", "708": "IL", "709": "NL", "712": "IA", "713": "TX",
                      "714": "CA", "715": "WI", "716": "NY", "717": "PA", "718": "NY", "719": "CO", "720": "CO",
                      "724": "PA", "727": "FL", "731": "TN", "732": "NJ", "734": "MI", "737": "TX", "740": "OH",
                      "747": "CA", "754": "FL", "757": "VA", "760": "CA", "762": "GA", "763": "MN", "764": "CA",
                      "765": "IN", "769": "MS", "770": "GA", "772": "FL", "773": "IL", "774": "MA", "775": "NV",
                      "778": "BC", "779": "IL", "780": "AB", "781": "MA", "782": "NS", "785": "KS", "786": "FL",
                      "787": "PR", "801": "UT", "802": "VT", "803": "SC", "804": "VA", "805": "CA", "806": "TX",
                      "807": "ON", "808": "HI", "810": "MI", "812": "IN", "813": "FL", "814": "PA", "815": "IL",
                      "816": "MO", "817": "TX", "818": "CA", "819": "QC", "825": "AB", "828": "NC", "830": "TX",
                      "831": "CA", "832": "TX", "835": "PA", "843": "SC", "845": "NY", "847": "IL", "848": "NJ",
                      "850": "FL", "856": "NJ", "857": "MA", "858": "CA", "859": "KY", "860": "CT", "862": "NJ",
                      "863": "FL", "864": "SC", "865": "TN", "867": "YT", "870": "AR", "872": "IL", "873": "QC",
                      "878": "PA", "901": "TN", "902": "NS", "903": "TX", "904": "FL", "905": "ON", "906": "MI",
                      "907": "AK", "908": "NJ", "909": "CA", "910": "NC", "912": "GA", "913": "KS", "914": "NY",
                      "915": "TX", "916": "CA", "917": "NY", "918": "OK", "919": "NC", "920": "WI", "925": "CA",
                      "927": "FL", "928": "AZ", "931": "TN", "935": "CA", "936": "TX", "937": "OH", "939": "PR",
                      "940": "TX", "941": "FL", "947": "MI", "949": "CA", "951": "CA", "952": "MN", "954": "FL",
                      "956": "TX", "957": "NM", "959": "CT", "970": "CO", "971": "OR", "972": "TX", "973": "NJ",
                      "975": "MO", "978": "MA", "979": "TX", "980": "NC", "984": "NC", "985": "LA", "989": "MI"}
        if not area_code:
            return choice(area_codes.items())[0]
        elif area_code:
            return area_codes[area_code]

    def get_devices_or_client_from_list(self, list_file, list_type="deviceIndex", spc=True, delimiter=",",
                                        l_type="CALLING", country="US"):
        list_file = list_file.replace("xlsx", "csv").replace("xls", "csv")
        devices = ["Device", "device", "homePhone", "workPhone", "cellPhone", "VacationPhone", "VoiceMail",
                   "email", "emailaddress", "workemail", "homeemail"]
        result = []
        if list_type == "deviceIndex" and (spc or l_type == "CALLING"):
            with open(list_file) as f:
                records = csv.DictReader(f, delimiter=delimiter)
                for list_rec in records:
                    result.extend(self.normalize_devices_in_string(list_rec, country=country))
                    emails = [v for k, v in dict(list_rec).iteritems() if (k and any(i in k for i in devices))
                              and ("@" in str(v))] if l_type is "CALLING" else ''
                    result += emails
        elif list_type == "ClientID" and spc:
            # regexp = re.compile("[0-9]{3}-[0-9]{2}-[0-9]{4}$")
            with open(list_file) as f:
                records = csv.DictReader(f)
                for list_rec in records:
                    if str(list_rec["ClientID"]) and list_rec["ClientID"] is not None:
                        result.append(list_rec["ClientID"])
        else:
            with open(list_file) as f:
                result = [line.strip() for line in f]
        return result

    def get_devices_from_file(self, list_file, incorrect=False, e_mails=True, country="US"):
        result = []
        regexp = re.compile("[A-Za-z0-9]{10}$")
        with open(list_file) as f:
            for list_rec in f:
                if incorrect:
                    if regexp.match(list_rec) and list_rec is not None:
                        result.append(list_rec.replace('\n', ''))
                else:
                    result.extend(self.normalize_devices_in_string(list_rec, country=country))
                    emails = [i.replace('"', '') for i in list_rec.split(',') if ("@" in i)] if e_mails else ''
                    result += emails
        return result

    @staticmethod
    def get_devices_from_list(list, list_type='CALLING', get_from_list="deviceIndex"):
        result = []
        if get_from_list == "deviceIndex":
            regexp = re.compile("^\+[0-9]{10,12}$")
            key = "contact_info" if list_type is 'CALLING' else "scd_device"
        else:
            regexp = re.compile("[0-9]{3}-[0-9]{2}-[0-9]{4}$")
            key = "scd_client_id"
        for list_rec in list:
            if regexp.match(str(list_rec[key])) and list_rec[key] is not None:
                result.append(list_rec[key])
            emails = [v for k, v in list_rec.items() if (key in k)and("@" in str(v))] if list_type is 'CALLING' else ''
            result += emails
        return result

    @staticmethod
    def get_devices_or_client_from_dict(list_of_dicts, list_type="deviceIndex", colling_list=False):
        result = []
        if list_type == "deviceIndex":
            regexp = re.compile("^\+[0-9]{10,11}$")
            for list_rec in list_of_dicts:
                if regexp.match(str(list_rec["scd_device"])) and list_rec["scd_device"] is not None:
                    result.append(list_rec["scd_device"])
        else:
            # regexp = re.compile("[0-9]{3}-[0-9]{2}-[0-9]{4}$")
            if colling_list:
                for list_rec in list_of_dicts:
                    if str(list_rec["c_client_id"]) and list_rec["c_client_id"] is not None:
                        result.append(list_rec["c_client_id"])
            else:
                for list_rec in list_of_dicts:
                    if str(list_rec["scd_client_id"]) and list_rec["scd_client_id"] is not None:
                        result.append(list_rec["scd_client_id"])
        return result

    def get_specific_devices_from_list(self, list_file, device_number='Device1'):
        list_file = list_file.replace("xlsx", "csv").replace("xls", "csv")
        result = []
        regexp = re.compile("[0-9]{3}-[0-9]{2}-[0-9]{4}$")
        with open(list_file) as f:
            records = csv.DictReader(f)
            for list_rec in records:
                if not regexp.match(str(list_rec[device_number])) and list_rec[device_number] is not None:
                    result.append(self.normalize_device(list_rec[device_number]))
        return result

    @staticmethod
    def normalize_device(device, support_email=False, country="US"):
        email = re.search(r'[\w\.-]+@[\w\.-]+', device)
        if email and support_email:
            return email.group(0)
        phone = phonenumbers.parse(device, country)
        normalized_device = (str(phonenumbers.format_number(phone, phonenumbers.PhoneNumberFormat.E164)))
        return normalized_device

    @staticmethod
    def normalize_devices_in_string(string, with_emails=False, country="US"):
        result = re.findall(r'[\w\.-]+@[\w\.-]+', str(string)) if with_emails else []
        for match in phonenumbers.PhoneNumberMatcher(str(string), country):
            number = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
            if len(number) > 10:
                result.append(number)
        return result

    def get_till_from_file(self, list_file, value_to_compare, records_type):
        till = None
        fromm = None
        with open(list_file) as f:
            records = csv.DictReader(f)
            for list_rec in records:
                if records_type == "deviceIndex":
                    if value_to_compare in self.normalize_devices_in_string(list_rec):
                        till = list_rec["till"]
                        fromm = list_rec["from"]
                else:
                    if value_to_compare in str(list_rec):
                        till = list_rec["till"]
                        fromm = list_rec["from"]
        return {'to': str(till), 'from': str(fromm)}

    def get_device_info(self, phone_number, return_value="all_info"):
        """
        :param phone_number: phone number
        :param return_value:
        : country_code: return Country Code of input phone number
        : area_code: return Area Code of input phone number
        : exchange: return Exchange of input phone number
        : state_code: return State Code of input phone number
        : time_zone: return Time Zone of input phone number
        : all_info: return {dict} with all phone number info
        :return: Country Code; Area Code; Exchange; State Code; Time Zone
        """
        try:
            number = self.normalize_device(phone_number)
            pn = phonenumbers.parse(number)
            pattern = re.compile(r'''(\d{3})\D*(\d{3})\D*(\d{4})\D*(\d*)$''', re.VERBOSE)
            num = pattern.search(str(pn.national_number)).groups()
            country_code = phonenumbers.region_code_for_number(pn)
            area_code = num[0]
            exchange = num[1]
            state_code = self.get_area_code(num[0])
            time_zone = str(phonenumbers.timezone.time_zones_for_number(pn))
            for ch in ['(', ',', ')', "'"]:
                time_zone = time_zone.replace(ch, '')
            info = {"country_code": country_code, "area_code": area_code, "exchange": exchange,
                    "state_code": state_code, "timezone": time_zone} if "all_info" in return_value else country_code \
                if "country_code" in return_value else area_code if "area_code" in return_value else exchange \
                if "exchange" in return_value else state_code if "state_code" in return_value else time_zone \
                if "timezone" in return_value else ""
            return info
        except Exception as e:
            Exception("Unable get info of {0}: {1}".format(phone_number, e))
