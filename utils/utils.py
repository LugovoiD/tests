import csv
import json
import os
import time
from random import choice, randint, sample
from string import ascii_letters, digits
from faker import Faker
import functools
import traceback
from unittest.case import SkipTest


def get_csv_data(file_name):
    rows = []
    file_path = os.path.join('api_aggregator/data', file_name)
    content = csv.reader(file(file_path))

    # skip header line
    next(content, None)

    # add rows to list
    for row in content:
        rows.append(row)
    return rows


class Singleton(type):

    def __init__(cls, name, bases, dict_object):
        super(Singleton, cls).__init__(name, bases, dict_object)
        cls.instance = None

    def __call__(cls, *args, **kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)

        return cls.instance


class StringUtils:
    def __init__(self):
        self.fake = Faker('en_US')

        self.countries = ['Afghanistan', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla',
                          'Antarctica', 'Antigua And Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia',
                          'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus',
                          'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia, Plurinational State Of',
                          'Bonaire, Saint Eustatius And Saba', 'Bosnia & Herzegovina', 'Botswana', 'Bouvet Island',
                          'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria',
                          'Burkina Faso', 'Burundi', 'Cabo Verde', 'Cambodia', 'Cameroon', 'Canada',
                          'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China',
                          'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Cook Islands',
                          'Costa Rica', 'Croatia', 'Cuba', 'Curacao', 'Cyprus', 'Czech Republic',
                          'Democratic Republic Of Congo', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic',
                          'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia',
                          'Falkland Islands', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'French Guiana',
                          'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia',
                          'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam',
                          'Guatemala', 'Guernsey', 'Guinea', 'Guinea-bissau', 'Guyana', 'Haiti',
                          'Heard Island And McDonald Islands', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland',
                          'India', 'Indonesia', 'Iran, Islamic Republic Of', 'Iraq', 'Ireland', 'Isle Of Man',
                          'Israel', 'Italy', 'Jamaica', 'Japan', 'Jersey', 'Jordan', 'Kazakhstan', 'Kenya',
                          'Kiribati', 'Korea, Republic Of', 'Kuwait', 'Kyrgyzstan', 'Latvia', 'Lebanon', 'Lesotho',
                          'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao',
                          'Macedonia, The Former Yugoslav Republic Of', 'Madagascar', 'Malawi', 'Malaysia',
                          'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius',
                          'Mayotte', 'Mexico', 'Micronesia, Federated States Of', 'Moldova', 'Monaco', 'Mongolia',
                          'Montenegro', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru',
                          'Nepal', 'Netherlands', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria',
                          'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan',
                          'Palau', 'Palestinian Territory, Occupied', 'Panama', 'Papua New Guinea', 'Paraguay',
                          'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar',
                          'Republic Of Congo', 'Reunion', 'Romania', 'Russian Federation', 'Rwanda',
                          'Saint Helena, Ascension And Tristan Da Cunha', 'Saint Kitts And Nevis', 'Saint Lucia',
                          'Saint Martin', 'Saint Pierre And Miquelon', 'Saint Vincent And The Grenadines', 'Samoa',
                          'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles',
                          'Sierra Leone', 'Singapore', 'Sint Maarten', 'Slovakia', 'Slovenia', 'Solomon Islands',
                          'Somalia', 'South Africa', 'South Georgia And The South Sandwich Islands', 'South Sudan',
                          'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'Svalbard And Jan Mayen', 'Swaziland',
                          'Sweden', 'Switzerland', 'Syrian Arab Republic', 'Taiwan', 'Tajikistan',
                          'Tanzania, United Republic Of', 'Thailand', 'Timor-Leste, Democratic Republic of', 'Togo',
                          'Tokelau', 'Tonga', 'Trinidad And Tobago', 'Tunisia', 'Turkey', 'Turkmenistan',
                          'Turks And Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates',
                          'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay',
                          'Uzbekistan', 'Vanuatu', 'Vatican City State', 'Venezuela, Bolivarian Republic Of',
                          'Viet Nam', 'Virgin Islands (British)', 'Virgin Islands (US)', 'Wallis And Futuna',
                          'Western Sahara', 'Yemen', 'Zambia', 'Zimbabwe']

        self.time_zone = \
            ["Africa/Accra", "Africa/Addis_Ababa", "Africa/Bangui", "Africa/Cairo", "Africa/Johannesburg",
             "America/Adak", "America/Anchorage", "America/Araguaina", "America/Bahia", "America/Belem",
             "America/Boa_Vista", "America/Buenos_Aires", "America/Campo_Grande", "America/Cancun", "America/Cayenne",
             "America/Chicago", "America/Chihuahua", "America/Denver", "America/Grand_Turk", "America/Halifax",
             "America/Hermosillo", "America/Indianapolis", "America/Los_Angeles", "America/Maceio", "America/Matamoros",
             "America/Mazatlan", "America/Merida", "America/Mexico_City", "America/Miquelon", "America/Monterrey",
             "America/New_York", "America/Ojinaga", "America/Phoenix", "America/Porto_Velho", "America/Puerto_Rico",
             "America/Recife", "Asia/Bangkok", "Asia/Anadyr", "Asia/Calcutta", "Asia/Dhaka", "Asia/Dubai",
             "Asia/Hong_Kong", "Asia/Irkutsk", "Asia/Jerusalem", "Asia/Kamchatka", "Asia/Karachi", "Asia/Krasnoyarsk",
             "Asia/Kuala_Lumpur", "Asia/Magadan", "Asia/Manila", "Asia/Novosibirsk", "Asia/Omsk", "Asia/Riyadh",
             "Asia/Seoul", "Asia/Singapore", "Asia/Taipei", "Asia/Tokyo", "Asia/Vladivostok", "Asia/Yakutsk",
             "Asia/Yekaterinburg", "Atlantic/Azores", "Atlantic/Reykjavik", "Australia/Adelaide", "Australia/Brisbane",
             "Australia/Darwin", "Australia/Hobart", "Australia/Perth", "Australia/Sydney", "Europe/Belgrade",
             "Europe/Dublin", "Europe/Helsinki", "Europe/Lisbon", "Europe/London", "Europe/Moscow", "Europe/Samara",
             "Indian/Reunion", "Pacific/Auckland", "Pacific/Easter", "Pacific/Guam", "Pacific/Honolulu",
             "Pacific/Pago_Pago"]

        self.regions = {'Mississippi': 'US-MS', 'Ontario': 'CA-ON', 'Palau': 'US-PW',
                        'Oklahoma': 'US-OK', 'Delaware': 'US-DE', 'Minnesota': 'US-MN', 'Micronesia': 'US-FM',
                        'Illinois': 'US-IL', 'British Columbia': 'CA-BC', 'Arkansas': 'US-AR', 'New Mexico': 'US-NM',
                        'New Hampshire': 'US-NH', 'Alberta': 'CA-AB', 'Indiana': 'US-IN', 'Maryland': 'US-MD',
                        'Louisiana': 'US-LA', 'Idaho': 'US-ID', 'Wyoming': 'US-WY', 'Northern Mariana Islands': 'US-MP',
                        'Armed Forces Europe, Canada, Africa and Middle East': 'US-AE', 'Tennessee': 'US-TN',
                        'Arizona': 'US-AZ', 'Iowa': 'US-IA', 'Newfoundland and Labrador': 'CA-NL',
                        'Saskatchewan': 'CA-SK', 'Michigan': 'US-MI', 'Kansas': 'US-KS', 'Utah': 'US-UT',
                        'American Samoa': 'US-AS', 'Oregon': 'US-OR', 'Prince Edward Island': 'CA-PE',
                        'Connecticut': 'US-CT', 'Montana': 'US-MT', 'California': 'US-CA', 'Massachusetts': 'US-MA',
                        'Puerto Rico': 'US-PR', 'South Carolina': 'US-SC', 'Marshall Islands': 'US-MH',
                        'Northwest Territories': 'CA-NT', 'Nunavut': 'CA-NU', 'Wisconsin': 'US-WI', 'Vermont': 'US-VT',
                        'Georgia': 'US-GA', 'North Dakota': 'US-ND', 'Pennsylvania': 'US-PA', 'West Virginia': 'US-WV',
                        'Florida': 'US-FL', 'Alaska': 'US-AK', 'Kentucky': 'US-KY', 'Hawaii': 'US-HI',
                        'Nebraska': 'US-NE', 'Nova Scotia': 'CA-NS', 'Armed Forces Pacific': 'US-AP',
                        'Missouri': 'US-MO', 'Ohio': 'US-OH', 'Alabama': 'US-AL', 'New York': 'US-NY',
                        'New Brunswick': 'CA-NB', 'Virgin Islands': 'US-VI', 'South Dakota': 'US-SD',
                        'Armed Forces Americas': 'US-AA', 'Colorado': 'US-CO', 'New Jersey': 'US-NJ',
                        'Virginia': 'US-VA', 'Guam': 'US-GU', 'Washington': 'US-WA', 'North Carolina': 'US-NC',
                        'District of Columbia': 'US-DC', 'Quebec': 'CA-QC', 'Texas': 'US-TX', 'Manitoba': 'CA-MB',
                        'Nevada': 'US-NV', 'Maine': 'US-ME', 'Rhode Island': 'US-RI', 'Yukon': 'CA-YT'}

        self.tz_for_location_rules = \
            ["America/Araguaina", "America/Bahia", "America/Belem", "America/Boa_Vista", "America/Campo_Grande",
             "America/Maceio", "America/Porto_Velho", "America/Recife", "Europe/Belgrade", "Africa/Bangui",
             "Atlantic/Reykjavik", "Africa/Accra", "Europe/London", "Europe/Dublin", "Europe/Lisbon", "Atlantic/Azores",
             "America/Buenos_Aires", "America/Cayenne", "America/Miquelon", "America/St_Johns", "America/Halifax",
             "America/Puerto_Rico", "America/Matamoros", "America/Monterrey", "America/Cancun",
             "America/Merida", "America/Ojinaga", "America/Chihuahua", "America/Hermosillo", "America/Santa_Isabel",
             "Asia/Omsk", "America/New_York", "America/Indianapolis", "America/Grand_Turk", "America/Santiago",
             "America/Chicago", "America/Regina", "America/Mexico_City", "America/Denver", "America/Phoenix",
             "America/Mazatlan", "America/Los_Angeles", "Pacific/Easter", "America/Anchorage", "Pacific/Honolulu",
             "America/Adak", "Pacific/Pago_Pago", "Pacific/Auckland", "Asia/Anadyr", "Asia/Kamchatka", "Pacific/Guam",
             "Australia/Sydney", "Australia/Brisbane", "Australia/Hobart", "Asia/Magadan", "Australia/Darwin",
             "Australia/Adelaide", "Asia/Seoul", "Asia/Tokyo", "Asia/Yakutsk", "Asia/Vladivostok", "Australia/Perth",
             "Asia/Hong_Kong", "Asia/Irkutsk", "Asia/Kuala_Lumpur", "Asia/Taipei", "Asia/Manila", "Asia/Singapore",
             "Asia/Bangkok", "Asia/Krasnoyarsk", "Asia/Calcutta", "Asia/Dhaka", "Asia/Novosibirsk", "Asia/Karachi",
             "Asia/Yekaterinburg", "Europe/Moscow", "Asia/Dubai", "Europe/Samara", "Indian/Reunion",
             "Europe/Kaliningrad", "Africa/Addis_Ababa", "Asia/Riyadh", "Europe/Helsinki", "Asia/Jerusalem",
             "Africa/Cairo", "Africa/Johannesburg"]
        self.locations =\
            ["CA-AB", "CA-BC", "CA-MB", "CA-NB", "CA-NL", "CA-NS", "CA-NT", "CA-NU", "CA-ON", "CA-PE", "CA-QC", "CA-SK",
             "CA-YT", "US-AK", "US-AL", "US-AR", "US-AS", "US-AZ", "US-CA", "US-CO", "US-CT", "US-DC", "US-DE",
             "US-FL", "US-GA", "US-GU", "US-HI", "US-IA", "US-ID", "US-IL", "US-IN", "US-KS", "US-KY", "US-LA", "US-MA",
             "US-MD", "US-ME", "US-MI", "US-MN", "US-MO", "US-MS", "US-MT", "US-NC", "US-ND", "US-NE", "US-NH", "US-NJ",
             "US-NM", "US-NV", "US-NY", "US-OH", "US-OK", "US-OR", "US-PA", "US-PR", "US-RI", "US-SC", "US-SD", "US-TN",
             "US-TX", "US-UT", "US-VA", "US-VI", "US-VT", "US-WA", "US-WI", "US-WV", "US-WY"]
        self.country_name = \
            ["AU", "AT", "AZ", "AX", "AL", "DZ", "VI", "AS", "AI", "AO", "AD", "AQ", "AG", "AR", "AM", "AW", "AF", "BS",
             "BD", "BB", "BH", "BZ", "BY", "BE", "BJ", "BM", "BG", "BO", "BQ", "BA", "BW", "BR", "IO", "VG", "BN", "BF",
             "BI", "BT", "VU", "VA", "GB", "HU", "VE", "UM", "TL", "VN", "GA", "HT", "GY", "GM", "GH", "GP", "GT", "GF",
             "GN", "GW", "DE", "GG", "GI", "HN", "HK", "GD", "GL", "GR", "GE", "GU", "DK", "JE", "DJ", "DM", "DO", "CD",
             "EG", "ZM", "EH", "ZW", "IL", "IN", "ID", "JO", "IQ", "IR", "IE", "IS", "ES", "IT", "YE", "CV", "KZ", "KY",
             "KH", "CM", "CA", "QA", "KE", "CY", "KG", "KI", "TW", "KP", "CN", "CC", "CO", "KM", "CR", "CI", "CU", "KW",
             "CW", "LA", "LV", "LS", "LR", "LB", "LY", "LT", "LI", "LU", "MU", "MR", "MG", "YT", "MO", "MK", "MW", "MY",
             "ML", "MV", "MT", "MA", "MQ", "MH", "MX", "FM", "MZ", "MD", "MC", "MN", "MS", "MM", "NA", "NR", "NP", "NE",
             "NG", "NL", "NI", "NU", "NZ", "NC", "NO", "AE", "OM", "BV", "IM", "CK", "NF", "CX", "PN", "SH", "PK", "PW",
             "PS", "PA", "PG", "PY", "PE", "PL", "PT", "PR", "CG", "KR", "RE", "RU", "RW", "RO", "SV", "WS", "SM", "ST",
             "SA", "SZ", "MP", "SC", "BL", "MF", "PM", "SN", "VC", "KN", "LC", "RS", "SG", "SX", "SY", "SK", "SI", "SB",
             "SO", "SD", "SR", "US", "SL", "TJ", "TH", "TZ", "TC", "TG", "TK", "TO", "TT", "TV", "TN", "TM", "TR", "UG",
             "UZ", "UA", "WF", "UY", "FO", "FJ", "PH", "FI", "FK", "FR", "PF", "TF", "HM", "HR", "CF", "TD", "ME", "CZ",
             "CL", "CH", "SE", "SJ", "LK", "EC", "GQ", "ER", "EE", "ET", "ZA", "GS", "SS", "JM", "JP"]
        self.used_time_zone = []

    @staticmethod
    def to_list(obj):
        return [] if obj is None else obj if isinstance(obj, list) or isinstance(obj, xrange) else list(
            obj) if isinstance(obj, tuple) else [obj]

    @staticmethod
    def rand_rule_fields():
        fields = ['First Name', 'Last Name', "Device1", "Device2", "Device3", "Device4",
                  "Device5", "Device6", "Device7", "Device8", "Device9", "Device10", "Other1", "Other2", "Other3",
                  "Other4", "Other5", "Other6", "Other7", "Other8", "Other9", "Client ID"]
        field = choice(fields)
        return field

    @staticmethod
    def rand_trigger_rule_campaign():
        campaigns = ['Default', 'example', 'multi-channel', 'multi-channel2', 'portal', 'smstest', 'voice']
        campaign = choice(campaigns)
        return campaign

    @staticmethod
    def rand_trigger_rule_timeframe():
        timeframes = ['Today', 'Next Day', 'Next Weekday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday',
                      'Friday', 'Saturday', 'Sunday']
        timeframe = choice(timeframes)
        return timeframe

    def rand_triggerrule_system_attribute(self):
        system_attributes = ['Actual list name', 'Actual list count after processing', 'Original contact count',
                             'Actual list modified date']
        system_attribute = choice(system_attributes)

        if system_attribute == 'Actual list name':
            return system_attribute, 'ListName', self.rand_string(7, ascii_letters)
        elif system_attribute == 'Actual list count after processing':
            return system_attribute, 'ListSize', randint(100, 99999)
        elif system_attribute == 'Original contact count':
            return system_attribute, 'FileSize', randint(100, 99999)
        elif system_attribute == 'Actual list modified date':
            date = (str(randint(2010, 2017))) + choice([('0' + str(randint(1, 9))), str(randint(10, 12))]) \
                   + choice([('0' + str(randint(1, 9))), str(randint(10, 30))])
            return system_attribute, 'ListModifiedDate', date

    @staticmethod
    def rand_type(field):
        if field in ["First Name", "Last Name", "Time Zone", "Post Code", "Country Code",
                     "State/Region", "Original Record", "Company Name"]:
            return "string"
        elif field in ["Other1", "Other2", "Other3", "Other4", "Other5", "Other6", "Other7", "Other8", "Other9",
                       "Client ID", "Order", "Condition"]:
            return choice(["string", "numeric"])
        elif field in ["Device1", "Device2", "Device3", "Device4", "Device5", "Device6", "Device7",
                       "Device8", "Device9", "Device10"]:
            return choice(["string", "area code", "country code", "exchange", "state code", "timezone"])
        elif field == "Default Device":
            return choice(["exchange", "area code"])

    @staticmethod
    def rand_operators(condition_type, except_operator=None):
        if condition_type in ["area code", "country code", "exchange", "state code", "timezone"]:
            operators = ["equal", "not equal", "in", "is empty"]
        elif condition_type == "numeric":
            operators = ["equal", "not equal", "less than", "less than or equal", "greater than",
                         "greater than or equal", "Custom JS Expression"]
        else:
            operators = ["equal", "not equal", "like", "not like", "in", "not in", "is empty", "contains",
                         "does not contain", "Custom JS Expression"]
        return choice([operator for operator in operators if operator != except_operator])

    def rand_rule_value(self, condition_type, operator, for_ui=False):
        if operator in ["equal", "not equal", "like", "not like", "contains", "does not contain", "less than",
                        "less than or equal", "greater than", "greater than or equal"] \
                and condition_type not in ["state code", "timezone", "country code"]:
            if condition_type in ["numeric", "area code", "exchange"]:
                return str(randint(1000, 9999))
            else:
                return self.rand_string(6, ascii_letters)
        elif condition_type in ["state code", "timezone", "country code"]:
            if operator == "in":
                if condition_type == "timezone":
                    return sample(self.time_zone, 2)
                elif condition_type == "state code":
                    return sample(self.regions.keys(), 2) if for_ui else [self.fake.state_abbr()]
                elif condition_type == "country code":
                    return sample(self.countries, 2) if for_ui else [self.fake.country_code()]
                else:
                    return sample(self.regions.keys(), 2) if for_ui else [self.fake.state_abbr() for _ in range(2)]
            elif operator in ["equal", "not equal"]:
                if condition_type == "timezone":
                    return choice(self.time_zone)
                elif condition_type == "state code":
                    return choice(self.regions.keys()) if for_ui else self.fake.state_abbr()
                elif condition_type == "country code":
                    return choice(self.countries) if for_ui else self.fake.country_code()
            elif operator == "is empty":
                return ""
        elif operator in ["in", "not in"]:
            if condition_type in ["numeric", "area code", "exchange"]:
                return "{0},{1},{2}".format(randint(1000, 9999), randint(1000, 9999), randint(1000, 9999))
            return "{0},{1},{2}".format(self.rand_string(6, ascii_letters), self.rand_string(6, ascii_letters),
                                        self.rand_string(6, ascii_letters))
        elif operator == "Custom JS Expression":
            return choice(["Mod 2 == 0", "Mod 2 == 1", "Mod 3 == 0", "Mod 3 == 1", "Mod 3 == 2"])
        elif operator == "is empty":
            return ""

    @staticmethod
    def rand_string(length, chars=ascii_letters + digits, prefix=""):
        return prefix + "".join(choice(chars) for _ in xrange(length))

    def rand_range_string(self, count, length, chars=ascii_letters + digits):
        result = []
        for i in xrange(0, count):
            result.append(self.rand_string(length, chars))
        return result

    @staticmethod
    def rand_range_int(count, a, b):
        result = []
        while len(result) < count:
            value = randint(a, b)
            if value in result:
                continue
            result.append(value)
        return result

    @staticmethod
    def rand_ip_address(ipv6=False):
        if ipv6:
            return ":".join("{:x}".format(randint(0, 2 ** 16 - 1)) for _ in xrange(8))
        return ".".join("{}".format(randint(0, 255)) for _ in xrange(4))

    @staticmethod
    def get_value_recursivly_from_dict(cfg_object_dict, keys, empty_value=None):
        if not isinstance(keys, list): keys = [keys]
        current_item = cfg_object_dict
        for current_key in keys:
            if current_item and ((len(current_item) > current_key) if isinstance(current_key, int)
            else current_item.has_key(current_key)):
                current_item = current_item[current_key]
            else:
                return empty_value
        return current_item

    @staticmethod
    def get_dict_from_list_of_dicts(list_of_dicts, expected_key, expected_value):
        for item in list_of_dicts:
            if item and expected_value is not None:
                if item.has_key(expected_key):
                    if item[expected_key] == expected_value:
                        return item
        return {}

    @staticmethod
    def get_dict_from_list(list_of_dicts, key):
        dict_of_elements = {key: {}}
        if key in list_of_dicts:
            for section in list_of_dicts[key]:
                dict_of_elements[key][section["key"]] = {}
                for key_value in section['value']:
                    dict_of_elements[key][section["key"]].update({key_value["key"]: key_value["value"]})
        return dict_of_elements

    def get_diff_in_lists(self, list_before, list_after):
        list_diff = [item for item in list_after if item not in list_before]
        return list_diff

    def formation_current_object_user_properties(self, object_properties):
        data = []
        for current_object in object_properties:
            if "userProperties" in current_object:
                current_object_properties = self.get_dict_from_list(current_object, "userProperties")
                current_object_properties = self.get_value_recursivly_from_dict(current_object_properties,
                                                                                'userProperties')
                if current_object_properties:
                    current_object['userProperties'] = current_object_properties
            data.append(current_object)
        return data

    @staticmethod
    def get_dict_list_from_list_of_dicts(list_of_dicts, expected_key, expected_value):
        result = []
        for item in list_of_dicts:
            if item and expected_value is not None:
                if item.has_key(expected_key):
                    if item[expected_key] == expected_value:
                        result.append(item)
        return result

    def rand_attempt_rule_location(self, to_from):
        if to_from in ['Area code']:
            return str(randint(100, 999))
        elif to_from in ['Region']:
            return choice(self.locations)
        elif to_from in ['Country']:
            return choice(self.country_name)
        else:
            pass

    @staticmethod
    def rand_cal_results():
        cal_results = [0, 1, 2, 3, 4, 5, 8, 10, 11, 12, 13, 14, 15, 16, 18, 19, 22, 23, 24, 25, 29, 30, 31, 34, 35,
                       36, 37, 38, 39, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52]
        result_of_choice = sample(cal_results, randint(1, 40))
        call_results = [6, 7, 9, 17, 20, 21, 26, 27, 32, 33, 40, 53] + result_of_choice
        return call_results

    def generate_csv_location(self, location_type):
        if location_type == 'regions':
            len_of_list = len(self.locations)
            csv_locations = sample(self.locations, randint(1, len_of_list))
            unique_csv_location = list(set(csv_locations) ^ set(sample(self.locations, randint(1, len_of_list))))
            size = len(unique_csv_location)
            wireless_regions = [i + "-W" for i in sample(unique_csv_location, randint(1, size))] if\
                unique_csv_location != [] else ''
            dnc_regions = [i + "-DNC" for i in sample(unique_csv_location, randint(1, size))] if \
                unique_csv_location != [] else ''
            region = {"csvLocations": csv_locations, "wirelessRegions": wireless_regions, "dncRegions": dnc_regions}
        elif location_type == 'timeZones':
            region = sample(self.tz_for_location_rules, randint(1, 50))
        elif location_type == 'countryCodes':
            region = list(set([self.fake.country_code() for _ in range(1, 10)]))
        elif location_type in ['areaCodes', 'postalCodes']:
            region = str(randint(1, 99))
        else:
            raise Exception('Incorrect location_type was given!! Must be one of ["areaCodes", "postalCodes", '
                            '"countryCodes", "timeZones", "regions"]')
        return region

    def generate_interval(self):
        return self.rand_string(2, digits) + ":" + self.rand_string(2, digits) + ":" + self.rand_string(2, digits)

    @staticmethod
    def generate_date(next_date=False):
        year = "2020-" if not next_date else "2021-"
        days = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]
        return year + choice(days) + "-" + choice([choice(days), str(randint(13, 28))]) + "T22:00:00.000Z"

    @staticmethod
    def rand_time_for_time_rule():
        hours = randint(00, 23)
        return str(hours if hours > 9 else '0' + str(hours)) + ":" + choice(["00", "15", "30", "45"]) + ":00"

    def return_name_of_region_for_contact_time(self):
        name = choice(self.locations)
        self.locations.remove(name)
        return name

    def time_zone_name(self):
        name = self.fake.timezone()
        if name in self.used_time_zone:
            self.time_zone_name()
        self.used_time_zone.append(name)
        return name

    @staticmethod
    def assert_status_code(r, expected_code):
        """
        Cant check http and status code, if expected code  list (first value  must be http code, second status code)
        :param r: response
        :param expected_code: code who are you waiting for
        """
        if isinstance(expected_code, list):
            assert r.status_code == expected_code[0], 'Actual http code - {0} not equal to expected values - {1}. ' \
                                                      'Actual response - {2}'.format(r.status_code, expected_code[0],
                                                                                     r.content)
            status_code = json.loads(r.text)['status']['code']
            assert status_code == expected_code[1], 'Actual status code - {0} not equal to expected values - {1}. ' \
                                                    'Actual response - {2}'.format(status_code, expected_code,
                                                                                   r.content)
            http_code = json.loads(r.text)['status']
            if "httpCode" in http_code:
                raise Exception("httpCode was found in response")

        else:
            assert r.status_code == expected_code, 'Actual http code - {0} not equal to expected values - {1}. ' \
                                                   'Actual response - {2}'.format(r.status_code, expected_code,
                                                                                  r.content)

    @staticmethod
    def assert_message_from_response(response, expected_message):
        message = json.loads(response.text)['status']['message']
        assert message == expected_message, "Unexpected message found: {0}. " \
                                            "Expected message: {1}".format(message, expected_message)

    @staticmethod
    def split_messages(msgs):
        """
        Splitting the list of messages/errors for line-by-line output
        :param msgs: [list] of messages
        """
        messages = '\n'+'\n'.join(str(i) for i in msgs) if isinstance(msgs, list) else msgs
        return messages

    def find_in_obj(self, obj, condition, path=None, new_value=None):
        """

        :param obj: object instance(list or dict)
        :param condition: key or list index
        :param path: path for list or dict (eg dict['data']['test'][0]
        :param new_value: new value for condition
        :return:
        """

        if path is None:
            path = []

            # In case this is a list
        if isinstance(obj, list):
            for index, value in enumerate(obj):
                new_path = list(path)
                new_path.append(index)
                for result in self.find_in_obj(value, condition, path=new_path, new_value=new_value):
                    yield result

            # In case this is a dictionary
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = list(path)
                new_path.append(key)
                for result in self.find_in_obj(value, condition, path=new_path, new_value=new_value):
                    yield result

                if condition == key:
                    new_path = list(path)
                    new_path.append(key)
                    obj[key] = new_value
                    yield new_path


def retry(exceptions=Exception, tries=3, delay=1):
    """
    Decorator to try function several times without razing an exception
    Usage:
        @retry(tries=3, delay=5)
        def files():
            pass
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: 3 times
    :param delay: initial delay between attempts. default: 5 seconds
    :returns: a retry decorator.
    """

    def retry_decorator(func):
        def func_wrapper(*args, **kwargs):
            _tries = tries
            while _tries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    _tries -= 1
                    if not _tries:
                        raise

                    time.sleep(delay)

        return func_wrapper

    return retry_decorator


class _SkipTest(SkipTest):
    def __init__(self, due, restart=False, trace=None):
        super(_SkipTest, self).__init__('{}\n{}\n{}'.format(due,  '-' * 10, trace))
        self.restart = restart
        self.due = due


def expected_failure(due, restart_browser=False):
    def test_wrapper(test):
        @functools.wraps(test)
        def inner(*args, **kwargs):
            try:
                test(*args, **kwargs)
            except:
                raise _SkipTest(due='Expected failure due {}'.format(due), restart=restart_browser,
                                trace=traceback.format_exc())
            else:
                args[0].fail('Unexpected test success accordingly to {}'.format(due))

        return inner
    return test_wrapper
