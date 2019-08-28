import commands
import csv
import json
from time import time, sleep
import zipfile
import gzip
from random import randint, choice, sample
from string import ascii_letters
from xlsxwriter.workbook import Workbook
from device_utils import DeviceUtils
from utils import StringUtils, retry
from logger import *
from multiprocessing import Process, Manager, current_process
from db_utils import DataBaseUtils


class FilesUtils(object):
    def __init__(self):
        self.string_utils = StringUtils()
        self.device_utils = DeviceUtils()
        self.db_utils = DataBaseUtils()

    def generate_supp_list(self, record_type="deviceIndex", records=None, incorrect=None):
        supp_file = []
        if record_type in ["deviceIndex", "ClientID"] or record_type is None:
            record_type = record_type if record_type else choice(["deviceIndex", "ClientID"])
            if incorrect and record_type == "deviceIndex":
                for i in range(incorrect):
                    supp_file.append(self.string_utils.rand_string(10))
            for record in range(records):
                if record_type == "deviceIndex":
                    line = self.device_utils.generate_number()
                else:
                    line = str(self.device_utils.fake.ssn())
                supp_file.append(line)
        else:
            raise Exception("Type is not the deviceIndex or Client Id\n")

        return supp_file

    @staticmethod
    def set_csv_header(dev_count=10, other_count=20, other_from=None, other_to=None, spec_fields=False):
        begin = "FirstName,LastName,Company,"
        end = "original_record,ClientID,from,till,Order,Condition"
        devices = ",".join("Device{0}".format(i) for i in range(1, dev_count+1))+","
        others = ",".join("Other{0}".format(i) for i in range(1, other_count+1))+"," if other_count else ""
        others_from_to = None if (other_from or other_to) is None \
            else ",".join("Other{0}".format(i) for i in range(other_from, other_to+1))
        others = others if others_from_to is None else others + others_from_to + ","
        headers = begin + devices + others + end
        return headers if not spec_fields else list(headers.split(","))

    def generate_client(self, header=True, correct=False, scdfrom=True, till=True, client_ids=True,
                        other_count=20, other_from=None, other_to=None, gb_number=False, seed=None, used_devices=None):
        def get_other():
            other = choice([True, False, False, False])
            return self.device_utils.fake.email() if other else ''

        device1 = self.device_utils.rand_device(can_be_none=False, correct=correct, gb_number=gb_number,
                                                used_devices=used_devices)
        devices_2_to_10 = ','.join('"' + self.device_utils.rand_device(correct=correct, gb_number=gb_number,
                                                                       used_devices=used_devices) +
                                   '"' for i in range(9))
        if header:
            if seed:
                self.device_utils.fake.random.seed(seed)
            first_name = str(self.device_utils.fake.first_name())
            last_name = str(self.device_utils.fake.last_name())
            company = str(self.device_utils.fake.company().replace(',', ''))
            other_1_to_ = ','.join('"' + get_other() + '"' for i in range(other_count)) if other_count else ""
            other_from_to_ = None if not(other_from or other_to) else \
                ','.join('"' + get_other() + '"' for i in range(other_from, other_to+1))
            list_others = other_from_to_ if not other_1_to_ else other_1_to_ if not other_from_to_ \
                else other_1_to_ + "," + other_from_to_
            original_record = self.string_utils.rand_string(6, ascii_letters)
            client_id = client_ids if client_ids is '' else str(self.device_utils.fake.ssn())
            till_from = sorted([randint(1, 86400), randint(1, 86400)])
            scd_from, till = str(min(till_from)) if scdfrom else '', str(max(till_from)) if till else ''
            order = condition = ''

            client = '"{0}","{1}","{2}","{3}",{4},{5},"{6}","{7}","{8}","{9}","{10}","{11}"'\
                .format(first_name, last_name, company, device1, devices_2_to_10, list_others, original_record,
                        client_id, scd_from, till, order, condition)
        else:
            client = '"{0}",{1}'.format(device1, devices_2_to_10)

        return client

    def generate_client_wrapper(self, calling_list_queue, correct_queue, seeds_queue, kwargs):
        try:
            while True:
                correct = correct_queue.get(False)
                seed = seeds_queue.get(False)
                record = self.generate_client(correct=correct, seed=seed, **kwargs)
                calling_list_queue.put(record)
        except Exception as e:
            if e.__class__.__name__ == 'Empty':
                return
            else:
                get_logger().error('{}: Unexpected error while calling list record generating:\n{}'.format(
                    current_process().name, str(e)))
                return

    def generate_calling_list(self, records_count, header, client_ids, empty_line=None, scdfrom=True, till=True,
                              other_count=20, other_from=None, other_to=None, gb_number=False, use_threading=True,
                              threads_count=None):
        start = time()
        calling_list = []
        if records_count > 500:
            not_correct = 50
        else:
            not_correct = records_count / 10

        if header:
            calling_list.append(self.set_csv_header(other_count=other_count, other_from=other_from, other_to=other_to))

        if empty_line:
            if isinstance(empty_line, int):
                for _ in xrange(empty_line):
                    calling_list.append("")
            else:
                calling_list.append("")
        if use_threading:
            threads_limit = 20
            manager = Manager()
            calling_list_q, correct_queue, seed_queue = manager.Queue(), manager.Queue(), manager.Queue()
            used_devices = Manager().list(sequence=())
            correct_incorrect = [False] * int(not_correct) + [True] * int(records_count - not_correct)
            [correct_queue.put(x) for x in correct_incorrect]
            start_seed = randint(1, 999999999999)
            [seed_queue.put(seed) for seed in range(start_seed, start_seed + records_count)]
            kwargs = {'scdfrom': scdfrom, 'till': till, 'client_ids': client_ids, 'other_count': other_count,
                      'other_from': other_from, 'other_to': other_to, 'header': header, 'gb_number': gb_number,
                      'used_devices': used_devices}

            threads_count = threads_count if threads_count else records_count if records_count < threads_limit else threads_limit
            workers = []
            for thread_num in range(threads_count):
                worker = Process(target=self.generate_client_wrapper, args=(calling_list_q, correct_queue, seed_queue,
                                                                            kwargs))
                worker.daemon = True
                worker.start()
                workers.append(worker)
            for worker in workers:
                worker.join()
            while not calling_list_q.empty():
                calling_list.append(calling_list_q.get())
        else:
            used_devices = []
            for record in range(int(not_correct)):
                calling_list.append(self.generate_client(scdfrom=scdfrom, till=till, client_ids=client_ids,
                                                         other_count=other_count, other_from=other_from, other_to=other_to,
                                                         header=header, gb_number=gb_number, used_devices=used_devices).replace('""', ''))
            for record in range(int(records_count - not_correct)):
                calling_list.append(self.generate_client(correct=True, scdfrom=scdfrom, till=till, client_ids=client_ids,
                                                         other_count=other_count, other_from=other_from, other_to=other_to,
                                                         header=header, gb_number=gb_number, used_devices=used_devices).replace('""', ''))
        get_logger().info("\r Calling list file [{}] created in {} seconds ({} thread(s))".format(
            records_count, round(time()-start), threads_count if use_threading else 1))
        return calling_list

    def make_file(self, list_type, name=None, extension=None, records_count=100, supp_list_type=None, spec_list=None,
                  header=True, empty_line=None, scdfrom=True, till=True, client_ids=True, incorrect=None,
                  other_count=20, other_from=None, other_to=None, gb_number=False, use_threading=True):
        name = name if name else self.string_utils.rand_string(6, ascii_letters)

        if list_type in ["calling list", "suppression"]:
            extensions = ["txt", "csv", "xls", "xlsx"]
            extension = extension if extension else choice(extensions[:-2])
            the_file = open(Config.Environment().get_attr("component") + '/files/{0}.{1}'.format(name,
                            extension if extension not in ['xls', 'xlsx'] else 'csv'), 'wb')
            the_list = self.generate_calling_list(
                records_count, header=header, empty_line=empty_line, other_count=other_count, other_from=other_from,
                other_to=other_to, scdfrom=scdfrom, till=till, client_ids=client_ids, gb_number=gb_number,
                use_threading=use_threading) \
                if list_type == "calling list" else self.generate_supp_list(
                incorrect=incorrect, record_type=supp_list_type if supp_list_type else None, records=records_count)
            for item in the_list:
                the_file.write("{}\n".format(item))
        if list_type == "specification":
            extension = extension if extension else "spc"
            the_file = open(Config.Environment().get_attr("component") + '/files/{0}.{1}'.format(name, extension), 'wb')
            the_list = self.generate_specification_list(field_list=spec_list)
            for item in the_list:
                the_file.write("{}\n".format(item))
        if list_type == 'timezone':
            extension = extension if extension else 'txt'
            the_file = open(Config.Environment().get_attr("component") + '/files/{0}.{1}'.format(name, extension), 'wb')
            the_list = self.generate_time_zone()
            tz_list = []
            for item in the_list:
                the_file.write("{}\n".format(item))
                tz_list.append(item.split(','))
            return {'path': the_file.name, 'list': tz_list}
        the_file.close()
        if extension in ["xls", "xlsx"]:
            file_name = self.csv_to_excel(the_file.name, extension)
            return file_name
        else:
            return the_file.name

    def generate_conditions(self, fields=[None], condition_types=[None], operators=[None], values=[None]):
        conditions_list = []
        for field, condition_type, operator, value in zip(fields, condition_types, operators, values):
            field = field if field is not None else self.string_utils.rand_rule_fields()
            condition_type = condition_type if condition_type is not None else \
                self.string_utils.rand_condition_type(field)
            operator = operator if operator is not None else self.string_utils.rand_operators(condition_type)
            value = value if value is not None else self.string_utils.rand_rule_value(condition_type, operator)
            condition = {"category": {"type": "List"}, "field": str(field), "externalId": "",
                         "fieldType": str(condition_type), "type": "Condition", "ruleTemplate": {"internalId": "8"},
                         "fieldCondition": str(operator), "fieldValue": str(value)}
            conditions_list.append(condition)
        return conditions_list

    def generate_rule_list(self, fields=[None], operators=[None], types=[None], values=[None]):
        rules_list = []
        for field, type, operator, value in zip(fields, types, operators, values):
            field = field if field is not None else self.string_utils.rand_rule_fields()
            type = type if type is not None else self.string_utils.rand_type(field)
            operator = operator if operator is not None else self.string_utils.rand_operators(type)
            value = value if value is not None else self.string_utils.rand_rule_value(type, operator)
            rule = {"field": str(field), "type": str(type), "operator": str(operator), "value": str(value)}
            rules_list.append(rule)
        return rules_list

    def generate_rule(self, conditions=None):
        rules = []
        conditions_list = []
        if not conditions:
            rule_set = []
            for j in range(randint(1, 3)):
                for i in range(randint(1, 3)):
                    rule_set += sorted(self.generate_rule_list())
                conditions_list.append(rule_set)
                rule_set = []
        else:
            conditions_list = conditions
        for rule in conditions_list:
            rules.append(rule)
        return rules

    def get_selection_definition(self):
        from api_utils.lists_utils import ListsUtils
        list_utils = ListsUtils()
        rule_dbid = {}
        sel_rule_info = {}
        selection_name = []
        for num in range(2):
            rules = self.generate_rule(conditions=False)
            sel_name = self.string_utils.rand_string(5)
            rule_id = list_utils.post_lists_rules_or_triggerrules(lists_type='rules', name=sel_name, rules=rules)
            rule = []
            for rule_set in rules:
                for cond in rule_set:
                    rule_cond = '%s %s %s %s' % (cond['field'], cond['operator'], cond['type'], str(cond['value']))
                    rule.append(rule_cond.replace('  ', ' '))
                    if cond == rule_set[-1]:
                        if rule_set != rules[-1]:
                            rule[-1] = rule[-1] + ' OR'
                    else:
                        rule[-1] = rule[-1] + ' AND'
            sel_rule_info.update({sel_name: rule})
            rule_dbid.update({sel_name: str(rule_id)})
            selection_name.append(sel_name)
        return {'dbid': rule_dbid, 'info': sel_rule_info, 'name': selection_name}

    def generate_specification_list(self, field_list=None):
        if field_list is None:
            field_list = []
            fields = self.set_csv_header().split(",")
            for i in range(randint(3, 6)):
                field_list.append(choice(fields))

        specification_list = []
        spec_line = "[\"]*([^\",]*)[\"]*.*$/$1/"
        for idx, field in enumerate(field_list):
            specification_list.append("# Pull {0} field from the {1} field "
                                      "(Fields are separated by commas)".format(field, str(idx + 1)))
            specification_list.append("{0}:s/^{1}".format(field, spec_line))
            specification_list.append("\n")
            spec_line = "[^,]*," + spec_line

        return specification_list

    def generate_time_zone(self):
        timezone_list = []
        for i in range(randint(2, 8)):
            country = randint(1, 9)
            area = randint(100, 999)
            timezone = self.device_utils.fake.timezone()
            timezone_list.append(str(country) + ',' + str(area) + ',' + str(timezone))
        return timezone_list

    @staticmethod
    def copy_file_to_container(file_type, source_file):
        domain = Config.Environment().get_attr("component")
        if domain == 'performance_tests':
            domain = 'list_builder'
        pod_name = Config.Environment().get_attr("pod_name")
        namespace = Config.Environment().get_attr("namespace")
        if namespace == 'local':
            if domain in ['api_aggregator', 'amark_ui']:
                r = commands.getoutput("docker cp {0} {1}:/etc/outbound-api/data/{2}/".format(os.path.abspath(source_file),
                                                                                              pod_name, file_type))
                assert r == '', "Failed to copy file to container using docker cp, response: {}".format(r)
            elif domain in ['job_scheduler']:
                r = commands.getoutput("docker cp {0} {1}:/{2}/input/{3}/".format(os.path.abspath(source_file),
                                                                                  pod_name, domain, file_type))
                assert r == '', "Failed to copy file to container using docker cp, response: {}".format(r)
            else:
                r = commands.getoutput("docker cp {0} {1}:/{2}/data/{3}/".format(os.path.abspath(source_file),
                                                                                 pod_name, domain, file_type))
                assert r == '', "Failed to copy file to container using docker cp, response: {}".format(r)

        else:
            if domain in ['api_aggregator', 'amark_ui']:
                m = commands.getoutput(
                    "kubectl exec --namespace={0} {1} -- mkdir -p /etc/outbound-api/data/{2}".format(namespace, pod_name,
                                                                                                     file_type))
                r = commands.getoutput("kubectl cp {0} {1}/{2}:/etc/outbound-api/data/{3}/".format
                                       (os.path.abspath(source_file), namespace, pod_name, file_type))
                assert r == "tar: removing leading '/' from member names" or r == '', \
                    "Failed to copy file to container using kubectl cp {0} {1}/{2}:/etc/outbound-api/data/{3}/," \
                    " Current response is: {4}".format(os.path.abspath(source_file), namespace, pod_name, file_type, r)
            elif domain in ['job_scheduler']:
                m = commands.getoutput("kubectl exec --namespace={0} {1} -- mkdir -p /{2}/input/{3}".format(namespace,
                                       pod_name, domain, file_type))
                r = commands.getoutput("kubectl cp {0} {1}/{2}:/{3}/input/{4}/".format
                                       (os.path.abspath(source_file), namespace, pod_name, domain, file_type))
                assert r == '', "Failed to copy file to container using kubectl cp, response: {}".format(r)
            else:
                m = commands.getoutput("kubectl exec --namespace={0} {1} -- mkdir -p /{2}/data/{3}".format
                                       (namespace, pod_name, domain, file_type))
                r = commands.getoutput("kubectl cp {0} {1}/{2}:/{3}/data/{4}/".format
                                       (os.path.abspath(source_file), namespace, pod_name, domain, file_type))
                assert r == "tar: removing leading '/' from member names" or r == '', \
                    "Failed to copy file to container using kubectl cp {0} {1}/{2}:/{3}/data/{4}/," \
                    " Current response is: {5}".format(os.path.abspath(source_file), namespace, pod_name, domain,
                                                       file_type, r)

    @staticmethod
    def copy_file_from_container(file_type, copy_file, path_to_file=None):
        domain = Config.Environment().get_attr("component")
        pod_name = Config.Environment().get_attr("pod_name")
        namespace = Config.Environment().get_attr("namespace")

        cwd = os.getcwd()
        copy_to = os.path.abspath("{0}/{1}/files/{2}".format(cwd, domain, copy_file))
        if namespace == 'local':
            if not file_type:
                path = domain if not path_to_file else path_to_file
                r = commands.getoutput("docker cp  {0}:/{1}/{2} {3}".format(pod_name, path, copy_file, copy_to))
            elif domain in ['job_scheduler']:
                r = commands.getoutput("docker cp  {0}:/{1}/input/{2}/{3} {4}".format(pod_name, domain, file_type,
                                                                                      copy_file, copy_to))
            else:
                r = commands.getoutput("docker cp  {0}:/{1}/data/{2}/{3} {4}".format(pod_name, domain, file_type,
                                                                                     copy_file, copy_to))
            assert r == '', "Failed to copy file from container using docker cp, response: {}".format(r)
        else:
            if not file_type:
                path = domain if not path_to_file else path_to_file
                r = commands.getoutput("kubectl cp {0}/{1}:/{2}/{3} {4}".format(namespace, pod_name, path,
                                                                                copy_file, copy_to))
            elif domain in ['job_scheduler']:
                r = commands.getoutput("kubectl cp {0}/{1}:/{2}/input/{3}/{4} {5}".format(namespace, pod_name, domain,
                                                                                          file_type, copy_file, copy_to))
            else:
                r = commands.getoutput("kubectl cp {0}/{1}:/{2}/data/{3}/{4} {5}".format(namespace, pod_name, domain,
                                                                                         file_type, copy_file, copy_to))
            assert r == "tar: removing leading '/' from member names" or r == '', \
                "Failed to copy file from container using kubectl cp {0}/{1}:/{2}/data/{3}/{4} {5}," \
                " Current response is: {6}".format(namespace, pod_name, domain, file_type, copy_file, copy_to, r)

        result = copy_to
        return result

    @staticmethod
    def check_file_in_container(file_type, file_name, exist=True, timeout=None, max_wait=20):
        domain = Config.Environment().get_attr("component")
        pod_name = Config.Environment().get_attr("pod_name")
        namespace = Config.Environment().get_attr("namespace")
        checked, start, r = False, time(), ''
        if timeout and not max_wait:
            sleep(timeout)

        if namespace == 'local':
            while not checked and (int(time()-start) < max_wait):
                get_logger().info("Checking file '{}' in container...".format(file_name))
                if domain in ['api_aggregator', 'amark_ui']:
                    r = commands.getoutput("docker exec {0} [ -f /etc/outbound-api/data/{1}/{2} ] && echo"
                                           " True || echo False".format(pod_name, file_type, file_name))
                elif domain in ['job_scheduler']:
                    r = commands.getoutput("docker exec {0} [ -f /{1}/input/{2}/{3} ] && echo"
                                           " True || echo False".format(pod_name, domain, file_type, file_name))
                else:
                    r = commands.getoutput("docker exec {0} [ -f /{1}/data/{2}/{3} ] && echo"
                                           " True || echo False".format(pod_name, domain, file_type, file_name))
                if r == str(exist):
                    get_logger().info("Check success")
                    checked = True
                else:
                    get_logger().info("Check failed...waiting 5 sec and check again...")
                    sleep(5)
            assert r == 'True' or r == 'False', "Failed to get file_exists(True or False)," \
                                                " current response is {}".format(r)
            if exist:
                assert r == 'True', 'File "{}" not exists, expected - exists'.format(file_name)
            else:
                assert r == 'False', 'File "{}" exists, expected - not exists'.format(file_name)
        else:
            while not checked and (int(time()-start) < max_wait):
                if domain in ['api_aggregator', 'amark_ui']:
                    r = commands.getoutput(
                        "kubectl exec --namespace={0} {1} -- test -e /etc/outbound-api/data/{2}/{3} && echo"
                        " True || echo False".format(namespace, pod_name, file_type, file_name))
                elif domain in ['job_scheduler']:
                    r = commands.getoutput(
                        "kubectl exec --namespace={0} {1} -- test -e /{2}/input/{3}/{4} && echo"
                        " True || echo False".format(namespace, pod_name, domain, file_type, file_name))
                else:
                    r = commands.getoutput(
                        "kubectl exec --namespace={0} {1} -- test -e /{2}/data/{3}/{4} && echo"
                        " True || echo False".format(namespace, pod_name, domain, file_type, file_name))
                if r == str(exist):
                    get_logger().info("Check success")
                    checked = True
                else:
                    get_logger().info("Check failed...waiting 5 sec and check again...")
                    sleep(5)

            assert r == 'True' or 'False' in r, "Failed to get file_exists(True or False)," \
                                                " current response is {}".format(r)
            if exist:
                assert r == 'True', 'File "{}" not exists, expected - exists'.format(file_name)
            else:
                assert 'False' in r, 'File "{}" exists, expected - not exists'.format(file_name)

    @staticmethod
    def get_files_in_container_by_path(path='/mnt/log', pod_name=None):
        pod_name = Config.Environment().get_attr("pod_name") if pod_name is None else pod_name
        namespace = Config.Environment().get_attr("namespace")

        if namespace == 'local':
            r = commands.getoutput("docker exec {0} ls {1}".format(pod_name, path))

        else:
            r = commands.getoutput("kubectl exec --namespace={0} {1} ls {2}".format(namespace, pod_name, path))

        assert 'No such file' not in r, 'Folder {0} not found in container {1}'.format(path, pod_name)
        return r.split('\n')

    @staticmethod
    def find_data_in_container_file(filename, data, pod_name=None):
        pod_name = Config.Environment().get_attr("pod_name") if pod_name is None else pod_name
        namespace = Config.Environment().get_attr("namespace")

        if namespace == 'local':
            r = commands.getoutput("docker exec {0} cat {1} | grep '{2}'".format(pod_name, filename, data))
        else:
            r = commands.getoutput("kubectl exec --namespace={0} {1} ls {2}".format(namespace, pod_name, filename))

        assert r != '', 'data {0} not found in file {1}, container {2}'.format(data, filename, pod_name)
        return r

    @staticmethod
    def change_ftp_folder_permissions():
        component = Config.Environment().get_attr("component")
        pod_name = Config.Environment().get_attr("pod_name")
        namespace = Config.Environment().get_attr("namespace")

        if namespace == 'local':
            if component in ['api_aggregator', 'amark_ui']:
                r = commands.getoutput("docker exec {} chmod 777 /etc/outbound-api/data/ftp".format(pod_name))
            elif component in ['job_scheduler']:
                r = commands.getoutput("docker exec {} chmod 777 /job_scheduler/input/ftp".format(pod_name))
        else:
            if component in ['api_aggregator', 'amark_ui']:
                r = commands.getoutput(
                    "kubectl exec --namespace={0} {1} -- sh -c 'mkdir -p /etc/outbound-api/data/ftp ; "
                    "chmod 777 /etc/outbound-api/data/ftp && chown"
                    " ftp /etc/outbound-api/data/ftp;'".format(namespace, pod_name))
            elif component in ['job_scheduler']:
                r = commands.getoutput(
                    "kubectl exec --namespace={0} {1} -- chmod 777 /job_scheduler/input/ftp".format(namespace,
                                                                                                    pod_name))

        assert r == '', "Failed to change folder permissions for ftp, response is: {}".format(r)

    @staticmethod
    def csv_file_to_dict(file_path, delimiter=","):
        csvlist = []
        with open(file_path) as f:
            records = csv.DictReader(f, delimiter=delimiter)
            for row in records:
                csvlist.append(row)
        return csvlist

    @staticmethod
    def get_key_client_id_and_device_from_dict(list_file):
        csvlist = []
        with open(list_file) as f:
            records = csv.DictReader(f)
            for row in records:
                for device_num in xrange(1, 10):
                    if row['Device{}'.format(device_num)] is not None or not '':
                        csvlist.append({row['Device{}'.format(device_num)]: row['ClientID']})
                        break
        return csvlist

    @retry(tries=4, delay=1)
    def validate_records_in_db_or_export(self, upload_files, download_file, ud_records=None, delimiter=",",
                                         export=False, check_devices=False, country="US"):
        upload_file = {}
        if not isinstance(upload_files, list):
            upload_files = upload_files.replace("xlsx", "csv").replace("xls", "csv")
            upload_file = self.csv_file_to_dict(upload_files, delimiter=delimiter)
        if not isinstance(download_file, list):
            download_file = self.csv_file_to_dict(download_file)
        # Get header from file
        header = self.get_header_from_file(list_file=upload_files, delimiter=delimiter)
        db_or_exp = "EXPORT" if export else "DB"
        all_kw = self.all_keywords()
        # All possible fields with devices in file
        all_devices = all_kw["all_devices"]+all_kw["e_mails"]
        # Fields with devices in file
        devices_in_file = [i for i in all_devices if i in header]
        # Fields for checking in DB/Export if it exist in header
        all_fields = {i: "c_client_id" if not export else "clientid" for i in all_kw["client_id"]}
        all_fields.update({i: "c_first_name" if not export else "firstname" for i in all_kw["first_name"]})
        all_fields.update({i: "c_last_name" if not export else "lastname" for i in all_kw["last_name"]})
        all_fields.update({i: "c_company" if not export else "company" for i in all_kw["company"]})
        # Check Devices in Export
        if export and (devices_in_file and check_devices):
            all_fields.update({i: i.lower() for i in devices_in_file})
        fields = {k: v for k, v in all_fields.iteritems() if k in header}
        get_logger().info("\r Validating '{0}' records in FILE and {1} . . .".format(country, db_or_exp))
        all_others = [i for i in header if "other" in i.lower()]
        ud_others = [item for item in all_others if int(str(item).lower().replace("other", '')) > 20]
        others = [item for item in all_others if item not in ud_others]

        ud_file, ud_upload = [], []
        if ud_records:
            ud_file = [{ud_rec["ud_key"]: ud_rec["ud_value"]} for ud_rec in ud_records if ud_rec["ud_value"] is not ""]
        errors = []
        written_records = len(download_file)
        redundant = list(download_file)
        for u_file in upload_file:
            ud_upload += [{'c_{0}'.format(i.lower()): u_file[i]} for i in ud_others if u_file[i] is not ""]
            devices = [",".join(u_file[device_] for device_ in devices_in_file if u_file[device_] is not "")]
            devices = self.device_utils.normalize_devices_in_string(devices, with_emails=True, country=country)
            other_upload = [",".join(u_file[other] for other in others)]
            for device in devices:
                for d_file in download_file:
                    other_db = [",".join(d_file["c_{}".format(i.lower()) if not export else i.lower()] for i in others)]
                    if d_file["contact_info"] in device:
                        for in_file, in_db, in fields.iteritems():
                            if "device" in str(in_file).lower() and u_file[in_file]:
                                u_file[in_file] = self.device_utils.normalize_devices_in_string(u_file[in_file],
                                                                                                with_emails=True)[0]
                            if u_file[in_file] != d_file[in_db]:
                                errors.append("[{0} in FILE: {1} not equals {2} in {3}: {4}]"
                                              "".format(in_file, u_file[in_file], in_db, db_or_exp, d_file[in_db]))
                        for other in zip(other_upload, other_db):
                            if other[0] != other[1]:
                                errors.append("[Other in FILE: {0} not equals to Other in {1}: {2}]"
                                              "".format(other[0], db_or_exp, other[1]))
                        else:
                            written_records -= 1
                            redundant.remove(d_file)
                            download_file.remove(d_file)
                            break
        if ud_records:
            wrong_in_db = [item for item in ud_file if item not in ud_upload]
            wrong_in_file = [item for item in ud_upload if item not in ud_file]
            assert wrong_in_db == wrong_in_file, "Records in DB: {0} not equals to File: {1}".format(wrong_in_db,
                                                                                                     wrong_in_file)
        assert errors == [], "Errors - {0}".format(errors).splitlines()
        assert written_records == 0, "Unexpected records are found! Records: {0}".format(redundant)

    def validate_count_of_records(self, export_file, records_count):
        if not isinstance(export_file, list):
            export_file = self.csv_file_to_dict(export_file)

        assert len(export_file) == records_count, 'Lines count of exported file is not equal to lines count in db. ' \
                                                  'Expected {0} to be equal {1}.' \
                                                  ' Exported file: {2}'.format(len(export_file), records_count,
                                                                               export_file[1:5])

    @staticmethod
    def csv_to_excel(csvfile, extension='xlsx'):
        workbook = Workbook(csvfile[:-4] + '.' + extension)
        worksheet = workbook.add_worksheet()
        with open(csvfile, 'rt') as f:
            reader = csv.reader(f)
            for r, row in enumerate(reader):
                for c, col in enumerate(row):
                    worksheet.write(r, c, col)
        workbook.close()
        return workbook.filename

    def trim_file(self, file_name, trimmed_lines=50):
        listik = []
        with open(file_name, 'rb') as f:
            for index, line in enumerate(f):
                if index < trimmed_lines:
                    listik.append(line)
        name = self.string_utils.rand_string(6)
        the_file = open(Config.Environment().get_attr("component") + '/files/{0}.{1}'.format(name, 'txt'), 'wb')
        for item in listik:
            the_file.write("{}\n".format(item.strip('\n')))
        the_file.close()
        return the_file.name

    def get_client_id_and_device_from_file(self, list_file, e_mails=True):
        """
        :param list_file: file for update.
        :return:  client_ids and devices from update file.
        """
        ids = []
        devices = []
        with open(list_file) as f:
            records = csv.DictReader(f)
            for list_rec in records:
                if list_rec['ClientID']:
                    ids.append(list_rec["ClientID"])
                for device_number in range(1, 11):
                    device = list_rec['Device{}'.format(str(device_number))]
                    devices.append(device)
            emails = [i for i in devices if i and ("@" in i)]
            devices = self.device_utils.normalize_devices_in_string(devices)
            devices = devices+emails if e_mails else devices
        return {'id': ids, 'device': devices}

    def check_duplicate_rec(self, list_file, db_result, device_before_update=[], dupl_exist=False, dupl_diff_files=False):
        """
        :param list_file: file for update
        :param db_result: records from DB after update
        :param device_before_update: contact numbers before update from DB
        :param dupl_exist: to check duplicate if needs
        :param dupl_diff_files: to check duplicate for different files
        :return: len of duplicate list
        """
        error = ''
        id_device = self.get_client_id_and_device_from_file(list_file)
        contact_before = None
        result_list = []
        dupl_cont = []
        # Parsing records from DB by ids from update file to check contact duplicates for one client and different for
        # update file or upload file.
        for list_rec in db_result:
            if list_rec['c_client_id'] in id_device['id'] and (len(filter(lambda cont:  cont == list_rec['contact_info'],
                                                                          id_device['device'])) > 1 or
                                                               filter(lambda y: y == list_rec['contact_info'],
                                                                      device_before_update)):
                result_list.append(list_rec)
        # Sort parsed list by cd_dol key.
        result_list = sorted(result_list, key=lambda k: k['cd_dol'])
        for list_rec in result_list:
            contact = list_rec['contact_info']
            if not dupl_diff_files:
                if contact_before == contact:
                    if contact in id_device['device']:
                        assert list_rec['cd_dol'] == 1, \
                          "Expected duplicate value: '{0}' doesn't match actual from" \
                          " db: '{1}' for ClientID: '{2}'".format(1, list_rec['cd_dol'], list_rec['c_client_id'])
                        dupl_cont.append(contact)
                    else:
                        error += "Expected contact info: '{0}' is not found in db" \
                                 " record: '{1}' for ClientID: '{2}' \n".format(id_device['device'], list_rec,
                                                                                list_rec['c_client_id'])
                else:
                    if contact in id_device['device']:
                        assert list_rec['cd_dol'] == 0, \
                          "Expected e: '{0}' doesn't match actual from db: '{1}' " \
                          "for ClientID: '{2}'".format(0, list_rec['cd_dol'], list_rec['c_client_id'])
                    else:
                        error += "Expected contact info: '{0}' is not found in db" \
                                 " record: '{1}' for ClientID: '{2}' \n".format(id_device['device'], list_rec,
                                                                                list_rec['c_client_id'])
                    contact_before = contact
            else:
                if contact in id_device['device']:
                    if dupl_exist:
                        assert list_rec['cd_dol'] == 1, \
                          "Expected duplicate value: '{0}' doesn't match actual from db:" \
                          " '{1}' for ClientID: '{2}'".format(1, list_rec['cd_dol'], list_rec['c_client_id'])
                        dupl_cont.append(contact)
                    else:
                        assert list_rec['cd_dol'] == 0, \
                          "Expected: '{0}' doesn't match actual from db: '{1}' for ClientID:" \
                          " '{2}'".format(0, list_rec['cd_dol'], list_rec['c_client_id'])
                else:
                    error += "Expected contact info: '{0}' is not found in db record:" \
                             " '{1}' for ClientID: '{2}' \n".format(id_device['device'], list_rec,
                                                                    list_rec['c_client_id'])
        if dupl_exist:
            if dupl_cont:
                return len(dupl_cont)
            else:
                error = "Not found duplicates in Database record"
        else:
            if dupl_cont:
                error = "Expected there is no duplicates in Database but found: {}".format(dupl_cont)
            else:
                return 0
        if error:
            raise Exception(error)

    @staticmethod
    def get_client_id_and_device_from_dict(list_of_dicts):
        """
        :param list_of_dicts: records from DB for calling lists.
        :return:  client ids, devices.
        """
        ids = []
        devices = []
        for list_rec in list_of_dicts:
            if list_rec["c_client_id"] and list_rec["c_client_id"] not in ids:
                ids.append(str(list_rec["c_client_id"]))
            if list_rec['contact_info']:
                devices.append(list_rec['contact_info'])
        return {'c_client_id': ids, 'contact_info': devices}

    @staticmethod
    def get_from_and_till_value_from_file(list_file):
        till = []
        froms = []
        with open(list_file) as f:
            records = csv.DictReader(f)
            for list_rec in records:
                if list_rec["till"]:
                    till.append(list_rec["till"])
                if list_rec["from"]:
                    froms.append(list_rec["from"])
        return {'till': till, 'from': froms}

    @staticmethod
    def get_all_value_from_supp_list_dict(list_of_dicts):
        """
        :param list_of_dicts: records from DB for supp lists.
        :return:  client ids, devices, till, from.
        """
        client_id, devices, till, froms = [], [], [], []
        for list_rec in list_of_dicts:
            client_id.append(str(list_rec["scd_client_id"])) if list_rec["scd_client_id"] else None
            devices.append(str(list_rec["scd_device"])) if list_rec["scd_device"] else None
            till.append(str(list_rec["scd_till"])) if list_rec["scd_till"] else None
            froms.append(str(list_rec["scd_from"])) if list_rec["scd_from"] else None
        return {'client_id': client_id, 'device': devices, 'till': till, 'from': froms}

    @retry(tries=5, delay=1)
    def check_artifacts_in_container(self, ccid, list_type, list_id, index, exist=True, extension='txt'):
        file_path = 'tenant_{0}/artifacts-out/{1}'.format(ccid, list_type)
        file_name = '{0}_{1}_0000_{2}.{3}'.format(ccid, list_id, index, extension)
        self.check_file_in_container(file_type=file_path, file_name=file_name, exist=exist)

    def compare_initial_file_and_file_from_container(self, original, ccid, list_type, list_id, index, extension='txt'):
        file_path = 'tenant_{0}/artifacts-out/{1}'.format(ccid, list_type)
        file_name = '{0}_{1}_0000_{2}.{3}'.format(ccid, list_id, index, extension)
        from_container = self.copy_file_from_container(file_type=file_path, copy_file=file_name)
        original_list = self.device_utils.get_devices_or_client_from_list(list_file=original, list_type=None)
        from_container_list = self.device_utils.get_devices_or_client_from_list(list_file=from_container,
                                                                                list_type=None)
        assert original_list == from_container_list, "File from container not equal to original file. " \
                                                     "Original file: {0}. File from" \
                                                     " container: {1}".format(original_list, from_container_list)

    def verify_file_after_request_fail(self, file_type, file_name):
        start_time = time()
        timeout_exception = start_time + 15
        while True:
            try:
                sleep(1)
                self.check_file_in_container(file_type=file_type, file_name=file_name)
                break
            except:
                get_logger().info('File is not found in container after {} seconds'.format(int(time() - start_time)))
            if time() > timeout_exception:
                raise Exception('File {} is not found in container after 15 seconds'.format(file_name))
        raise Exception('File {0} is found in container after {1} seconds'.format(file_name,
                                                                                  int(time() - start_time)))

    def make_advanced_spec_file(self, **kwargs):
        """
        Make specification file with "standard" fields if call without parameters
        :param kwargs:
        name: file name
        field_list: if need spec file with custom fields
        option_name and option_value: if need spec file with option specifiers
        concat_to and concat_fields: if need spec file with concatenation
        dict_for_spec: [dict] with custom fields and column number(ex.: {"clientid": "8",...})
        :return: specification file name
        """
        name = "adv_spec_{0}".format(self.string_utils.rand_string(6, ascii_letters)) if 'name' not in kwargs.keys()\
            else kwargs['name']
        fields = self.set_csv_header(spec_fields=True) if 'field_list' not in kwargs.keys()\
            else kwargs['field_list']
        field_list = fields if isinstance(fields, list) else fields.split(',')
        dict_for_spec = None if 'dict_for_spec' not in kwargs.keys() else kwargs['dict_for_spec']
        # Options - values: CSVFieldSeparator '\t', HeaderCount 0, TrailerCount 99, ErrorLimit 2, ErrorLimitPercent 4,
        # FixedWidth 700, CreditCardCheck false, ClientIDHash true, CallerID 18004552020
        option_name = None if 'option_name' not in kwargs.keys() else kwargs['option_name']
        option_value = None if 'option_value' not in kwargs.keys() else kwargs['option_value']
        # concatenation field name
        concat_to = False if 'concat_to' not in kwargs.keys() else kwargs['concat_to']
        # fields names, which need concatenate in "concat_to" field
        concat_fields = None if 'concat_fields' not in kwargs.keys() else kwargs['concat_fields']
        con_fields_num, con_msg = [], ""
        # generating concatenation fields numbers and message
        if concat_fields is not None:
            con_fields_num = [(str(idx+1) if idx+1 < 10 else "({0})".format(idx+1))
                              for item in concat_fields for idx, field in enumerate(field_list) if item == field]
            con_msg = "# Advanced: {0} is the concatenation of {1} of input record\n".format(concat_to, concat_fields)
        the_file = open(Config.Environment().get_attr("component") + '/files/{0}.{1}'.format(name, "spc"), 'wb')
        # add option specifiers to spec
        if (option_name and option_value) is not None:
            the_file.writelines("Option: {0} {1}\n".format(option_name, option_value))
        # generating spec file
        if dict_for_spec:
            for k, v in dict_for_spec.iteritems():
                the_file.writelines("#\n{0}:s/<CSV>/${1}/\n".format(k, v))
        else:
            for idx, field in enumerate(field_list):
                idx = str(idx + 1) if (idx + 1 < 10) else str("({0})".format(idx + 1))
                if field == concat_to and concat_fields is not None:
                    the_file.write(con_msg)
                    idx = ",$".join(con_fields_num)
                the_file.writelines("#\n{0}:s/<CSV>/${1}/\n".format(field, idx))

        the_file.close()
        return the_file.name

    @staticmethod
    def all_keywords():
        """
        :return: specification keywords
        """
        # Specification Keywords:
        # https://intranet.genesys.com/pages/viewpage.action?spaceKey=RP&title=HLD%3A+List+Builder#HLD:ListBuilder-SpecificationKeywords
        kw = \
            {"first_name": ["FirstName", "fname", "first name", "firstname"],
             "last_name": ["LastName", "lname", "last name", "lastname", "name"],
             "company": ["Company", "company name", "company", "companyname"],
             "client_id": ["ClientID", "clientid", "client id"],
             "original": ["original record", "originalrecord", "original", "original_record"],
             "time_zone": ["tz", "timezone", "time_zone"],
             "postal_code": ["zip", "zip_code", "zip code", "postal_code", "postal code"],
             "country_code": ["country", "country_code", "country code"],
             "state": ["state", "region", "state code", "statecode", "state_code"],
             "Devices": ["Device{}".format(i) for i in range(1, 11)],
             "devices": ["device{}".format(i) for i in range(1, 11)],
             "ci_devices": ["homePhone", "workPhone", "cellPhone", "VacationPhone", "VoiceMail"],
             "e_mails": ["email", "emailaddress", "workemail", "homeemail"]}
        kw.update({"all_devices": kw["devices"]+kw["Devices"]+kw["ci_devices"]})

        return kw

    @staticmethod
    def get_header_from_file(list_file, delimiter=","):
        """
        :param list_file: source file
        :param delimiter: delimiter in source file
        :return: header from file
        """
        with open(list_file) as f:
            header_row = (csv.reader(f)).next()
            header = str(header_row[0]).split(delimiter) if (len(header_row) == 1) and (delimiter is not ",") \
                else header_row
        return header

    def make_advanced_list_file(self, **kwargs):
        """
        Make list file with "standard" fields if call without parameters
        :param kwargs:
        locale: "en_US" - 'faker' locale for generating fields of some country (example: 'en_AU' for Australia)
        name: file name
        extension: output file extension
        empty_file: if need empty file
        header_row: header row on the basis of which the file is generated
        records: count of records in the file
        device_count: count of devices in each records
        other_count: count of others fields in each records
        write_header: True if need header row in list file
        write_line: if need add custom text to file
        write_devices: True if need devices in list file
        write_others: True if need others fields in list file
        others_length: [int] length of each other fields in file
        devices_correct: True - all devices in file will be correct phone numbers besides "wrong_records"
        devices_wrong: True - all devices in file will be wrong
        wrong_records: range of wrong records in file (example: [5,6,7,8] - in 5-8 records in file wil be wrong devices)
        separator: if need custom separator for option specifiers or dsv files
        empty_lines_before: [int] add empty lines before all records
        empty_lines_after: [int] add empty lines after all records
        :return: list file name
        """
        start = time()
        name = "adv_list_{0}".format(self.string_utils.rand_string(6, ascii_letters)) if 'name' not in kwargs.keys() \
            else kwargs['name']
        extension = choice(["txt", "csv"]) if 'extension' not in kwargs.keys() else kwargs['extension']
        the_file = open(Config.Environment().get_attr("component") + '/files/{0}.{1}'.format(name, extension), 'wb')
        empty_file = False if 'empty_file' not in kwargs.keys() else kwargs['empty_file']
        records = 0 if empty_file else 50 if 'records' not in kwargs.keys() else kwargs['records']
        client_ids, devices, country = [], [], ''
        if not empty_file:
            from faker import Faker
            locale = "en_US" if 'locale' not in kwargs.keys() else kwargs['locale']
            fake = Faker(locale)
            country = locale[3:]
            write_header = True if 'write_header' not in kwargs.keys() else kwargs['write_header']
            write_line = False if 'write_line' not in kwargs.keys() else kwargs['write_line']
            write_devices = True if 'write_devices' not in kwargs.keys() else kwargs['write_devices']
            devices_correct = False if 'devices_correct' not in kwargs.keys() else kwargs['devices_correct']
            devices_wrong = False if 'devices_wrong' not in kwargs.keys() else kwargs['devices_wrong']
            wrong_records = [] if 'wrong_records' not in kwargs.keys() else kwargs['wrong_records']
            write_others = True if 'write_others' not in kwargs.keys() else kwargs['write_others']
            others_length = False if 'others_length' not in kwargs.keys() else kwargs['others_length']
            header = self.set_csv_header() if 'header_row' not in kwargs.keys() else kwargs['header_row']
            header_row = header if isinstance(header, list) else header.split(',')
            all_kw = self.all_keywords()
            others_in_file = [i for i in header_row if "other" in i.lower()]
            quotes = '"' if 'quotes' not in kwargs.keys() else kwargs["quotes"]
            separator = "," if 'separator' not in kwargs.keys() else kwargs['separator']
            empty_lines_before = False if 'empty_lines_before' not in kwargs.keys() else kwargs['empty_lines_before']
            empty_lines_after = False if 'empty_lines_after' not in kwargs.keys() else kwargs['empty_lines_after']
            # adding header to file
            if write_header:
                the_file.writelines(separator.join(header_row))
                the_file.write("\n")
            if write_line:
                the_file.writelines("{0}\n".format(str(write_line)))
            # adding empty lines
            if empty_lines_before:
                for _ in range(empty_lines_before):
                    the_file.write("\n")
            # generating records based on header row and params in kwargs
            for record in range(records):
                client = []
                for field in header_row:
                    f_name = fake.first_name() if field in all_kw["first_name"] else None
                    l_name = fake.last_name() if field in all_kw["last_name"] else None
                    co_name = str(fake.company()).replace(',', '') if field in all_kw["company"] else None
                    device, other, e_mail, c_id = None, None, None, None
                    correct_device = True if "device1" == field.lower() else False
                    if write_devices and (field in all_kw["all_devices"]):
                        empty_device = choice([True, False, True])
                        if devices_wrong or record+1 in wrong_records:
                            device = self.string_utils.rand_string(10)
                        elif (not devices_correct and not correct_device) and empty_device:
                            device = ""
                        else:
                            correct, device_, pn_gen = False, '', time()
                            while not correct and int(time()-pn_gen) < 2:
                                phone_or_email = choice([True, False])
                                if (devices_correct or correct_device) or phone_or_email:
                                    device_ = self.device_utils.generate_number() if locale == "en_US" \
                                        else fake.phone_number()
                                    # in case of local Australian number without area code
                                    if locale == "en_AU" and len(device_) < 10:
                                        device_ = "{0}{1}".format(choice(['02', '03', '04', '07', '08']), device_)
                                    normalized = self.device_utils.normalize_devices_in_string(device_, country=country)
                                    # In case of device duplication
                                    if normalized and (normalized[0] not in devices):
                                        devices.append(normalized[0])
                                        correct = True
                                else:
                                    d_email = fake.email()
                                    # In case of e-mail device duplication
                                    device_ = d_email if d_email not in devices else "new_{0}".format(d_email)
                                    correct = True
                            device = device_
                    if write_others and (field in others_in_file):
                        yes_or_no = choice([True, False, False, False])
                        try:
                            other_ = str(choice([fake.year(), fake.street_address(), fake.country(), fake.city(),
                                                 fake.job(), fake.email()])).replace('\n', '') if yes_or_no else ''
                        except Exception as e:
                            get_logger().info(e)
                            other_ = fake.email() if yes_or_no else ''
                        other = self.string_utils.rand_string(others_length) if others_length \
                            else str(other_).replace(',', '')
                    orig = self.string_utils.rand_string(6, ascii_letters) if field in all_kw["original"] else None
                    if field in all_kw["e_mails"]:
                        e_mail_ = fake.email()
                        # In case of e-mail duplication
                        e_mail = e_mail_ if e_mail_ not in devices else "{0}_{1}".format(fake.word(), e_mail_)
                        if e_mail:
                            devices.append(e_mail)
                    tz = fake.timezone() if field in all_kw["time_zone"] else None
                    z_code = fake.zipcode() if field in all_kw["postal_code"] else None
                    c_code = fake.country_code() if field in all_kw["country_code"] else None
                    region = fake.state_abbr() if field in all_kw["state"] else None
                    if field in all_kw["client_id"]:
                        id_ = '' if 'empty_client_ids' in kwargs.keys() else randint(1111111, 9999999) \
                               if "numeric_ids" in kwargs.keys() else "{0}-{1}".format(country, fake.ssn())
                        #  In case of ClientID duplication
                        c_id = id_ if id_ not in client_ids else "{}-D".format(id_)
                        if c_id:
                            client_ids.append(c_id)
                    from_ = (randint(1, 40000) if "from" not in kwargs.keys() else kwargs['from']) \
                        if field == 'from' else None
                    till = (randint(40001, 86400) if "till" not in kwargs.keys() else kwargs['till']) \
                        if field == 'till' else None
                    order = ('' if "order" not in kwargs.keys() else kwargs['order']) if field == 'Order' else None
                    cond = ('' if "condition" not in kwargs.keys() else kwargs['condition']) \
                        if field == 'Condition' else None
                    client_field = [i for i in [f_name, l_name, co_name, device, other, orig, e_mail, tz, z_code,
                                                c_code, region, c_id, from_, till, order, cond] if i is not None]
                    client += client_field
                the_file.write(separator.join(quotes+str(i)+quotes for i in client))
                the_file.write("\n")
            # adding empty lines
            if empty_lines_after:
                for _ in range(empty_lines_after):
                    the_file.write("\n")
        the_file.close()
        rec_count = "EMPTY" if empty_file else "{0} '{1}' records".format(records, country)
        get_logger().info("\r File >> {0} << [{1}] created in {2} seconds"
                          "".format(the_file.name, rec_count, round(time()-start)))
        return the_file.name

    def make_zip_or_gzip(self, files, **kwargs):
        """
        Name must be with extension of file
        :param files: file was need to archive
        :param kwargs: for example (name='abc')
        :return: path to already archived file
        """
        name = self.string_utils.rand_string(6, ascii_letters) + '.txt' if 'name' not in kwargs.keys() else kwargs[
            'name']
        extension = choice(["zip", "gz"]) if 'extension' not in kwargs.keys() else kwargs['extension']
        zipped_file = str(Config.Environment().get_attr("component") + '/files/{0}.{1}'.format(name, extension))
        if extension == 'zip':
            with zipfile.ZipFile(os.path.abspath(zipped_file), "w") as the_file:
                the_file.write(files, os.path.basename(files))
        elif extension == 'gz':
            with gzip.GzipFile(os.path.abspath(zipped_file), "wb") as the_file:
                the_file.write(open(files).read())
        else:
            raise Exception("Extension not in 'zip' or 'gz'.")
        the_file.close()
        return zipped_file

    def get_file_info_for_rules(self, upload_file, rule_field="First Name", rule_type="string", rule_op="equal",
                                value_count=5, db_rec=None, rule_value=None, return_all_values=False,
                                upload_mode="CREATE", update_file=False):
        """
        Search value in file and count the number of this value in file by selection rule parameters
        :param upload_file: list file with header
        :param update_file: list file with header for updating list in upload_mode
        :param upload_mode: Upload mode("APPEND_AND_UPDATE", "APPEND_ONLY", "FLUSH_APPEND")
        :param rule_field: in which field search the value
        :param rule_type: selection rule type
        :param rule_op: selection rule operator
        :param rule_value: selection rule value
        :param value_count: count of values, uses only with operator "in" and "not in"
        :param db_rec: [values] from single column from DB, necessary for "state_code", "country_code" and "time_zone"
        :param return_all_values: return all [values] from selected row "rule_field" in upload_file
        :return: {dict} with value and count of this value in file
        """
        if not upload_file:
            raise Exception("Upload file not exist")
        if upload_mode != "CREATE" and not update_file:
            raise Exception("File for upload mode {} not exist".format(upload_mode))
        device_types = ["area_code", "exchange", "state_code", "country_code", "time_zone", "timezone"]
        rule_type = str(rule_type).replace(" ", '_')
        rule_field = str(rule_field).replace(' ', '')
        list_file = csv.DictReader(file(upload_file))
        rec_count = len([(row[rule_field]) for row in (csv.DictReader(file(upload_file)))])
        if upload_mode in ["APPEND_AND_UPDATE", "APPEND_ONLY", "appendOnly", "appendAndUpdate"]:
            list_file = [row for row in csv.DictReader(file(upload_file))]
            upload_ids = [row["ClientID"] for row in csv.DictReader(file(upload_file))]
            update = [row for row in csv.DictReader(file(update_file)) if row["ClientID"] not in upload_ids]
            list_file.extend(update)
            rec_count = len(list_file)
        if upload_mode in ["FLUSH_APPEND", "flushAndAppend"]:
            list_file = csv.DictReader(file(update_file))
            rec_count = len([row for row in (csv.DictReader(file(update_file)))])
        values = [(self.device_utils.get_device_info(phone_number=row[rule_field], return_value=rule_type))
                  for row in list_file] if (rule_type in device_types and not db_rec) \
            else db_rec if (rule_type in device_types and db_rec) else [(row[rule_field]) for row in list_file]
        if return_all_values:
            return values
        records = list(set(x for x in values if x))
        get_logger().info("\nALL/unique values: {0}/{1} : {2}".format(len(values), len(set(values)), values))
        samples = (value_count if (len(records) >= int(value_count)) else len(records)) \
            if (rule_op in ["in", "not in"] and records) else 0
        value = "" if rule_op == "is empty" else sample(records, samples) if (rule_op in ["in", "not in"]) \
            else rule_value if rule_value else choice(records)
        less = 0 if rule_type != "numeric" else len([item for item in values if item < value])
        greater = 0 if rule_type != "numeric" else len([item for item in values if item > value])
        v_count = sum([values.count(x) for x in value]) if (rule_op in ["in", "not in"]) else values.count(value)
        count = (rec_count - v_count) if rule_op in ["not in", "not like", "does not contain", "not equal"] \
            else v_count if rule_op in ["equal", "like", "contains", "in", "is empty"] \
            else less if rule_op in ["less than"] else (less+v_count) if rule_op in ["less than or equal"] \
            else greater if rule_op in ["greater than"] \
            else (greater+v_count) if rule_op in ["greater than or equal"] else 0
        rule_type = rule_type.replace('_', '') if rule_type == "time_zone" else rule_type.replace('_', ' ')
        value = (",".join(value)) if (rule_type not in device_types and rule_op in ["in", "not in"]) else value
        rule = [{"field": rule_field.lower(), "type": rule_type, "operator": rule_op, "value": value}]
        get_logger().info(" Upload mode: {6} | {0} [{1}] {2} [{3}] | = [ {4}/{5} ] entries"
                          "".format(rule_field, rule_type, rule_op, value, count, rec_count, upload_mode))
        return {"value": value, "count": count, "rule": rule}

    @staticmethod
    def get_info_from_json_file(json_file, info='dependencies'):
        """
        :param json_file: path to .json file
        :param info: if need some dict from file
        :return: json file converted to dict
        """
        with open(json_file) as f:
            json_data = json.loads(f.read())
        return json_data[info] if info else json_data

    def check_in_db_add_contact_to_cl(self, lists_id, payload):
        """
        Check all keywords in database for adding one entry for calling list
        :param lists_id: BDID of calling list
        :param payload: payload from request
        """

        db_numbers, db_f_name, db_l_name, db_company = [], [], [], []
        # OCS fields check
        db_record_type, db_dial_sched_time, db_campaign_id, db_group_id, db_agent_id = [], [], [], [], []
        record_type = payload['data']['fields']['record_type'] if 'record_type' in payload['data']['fields'] \
            else None
        dial_sched_time = payload['data']['fields']['dial_sched_time'] \
            if 'dial_sched_time' in payload['data']['fields'] else None
        campaign_id = payload['data']['fields']['campaign_id'] if 'campaign_id' in payload['data']['fields'] \
            else None
        group_id = payload['data']['fields']['group_id'] if 'group_id' in payload['data']['fields'] else None
        agent_id = payload['data']['fields']['agent_id'] if 'agent_id' in payload['data']['fields'] else None
        numbers = []
        for _ in range(1, 11):
            if 'Device{}'.format(_) in payload['data']['fields']:
                num = self.device_utils.normalize_device(payload['data']['fields']['Device{}'.format(_)])
                numbers.append(num)
        f_name = payload['data']['fields']['FirstName']
        l_name = payload['data']['fields']['LastName']
        company = payload['data']['fields']['Company'] if 'Company' in payload['data']['fields'] else None
        table_name = 'cc_list_{}'.format(lists_id)
        client_id = payload['data']['fields']['ClientID']
        record_from_db = self.db_utils.get_records_from_db_with_parameters(table_name=table_name)
        for item in record_from_db:
            if item['c_client_id'] == client_id:
                db_numbers.append(item['contact_info'])
                db_f_name.append(item['c_first_name'])
                db_l_name.append(item['c_last_name'])
                db_company.append(item['c_company']) if 'Company' in payload['data']['fields'] else None
                db_record_type.append(item['record_type']) if 'record_type' in payload['data']['fields'] else None
                db_dial_sched_time.append(item['dial_sched_time']) \
                    if 'dial_sched_time' in payload['data']['fields'] else None
                db_campaign_id.append(item['campaign_id']) if 'campaign_id' in payload['data']['fields'] else None
                db_group_id.append(item['group_id']) if 'group_id' in payload['data']['fields'] else None
                db_agent_id.append(item['agent_id']) if 'agent_id' in payload['data']['fields'] else None
        assert [f_name] == list(set(db_f_name)), "Incorrect FirstName found in db. Expected: {0}. Actual: " \
                                                 "{1}".format(f_name, db_f_name)
        assert [l_name] == list(set(db_l_name)), "Incorrect LastName found in db. Expected: {0}. Actual: " \
                                                 "{1}".format(l_name, db_l_name)
        if 'Company' in payload['data']['fields']:
            assert [company] == list(set(db_company)), "Incorrect Company found in db. Expected: {0}. Actual:" \
                                                       " {1}".format(company, db_company)
        assert sorted(numbers) == sorted(db_numbers), "Incorrect device found in db. Expected: {0}. Actual:" \
                                                      " {1}".format(sorted(numbers), sorted(db_numbers))
        if 'record_type' in payload['data']['fields']:
            assert [record_type] == list(set(db_record_type)), "Incorrect record_type found in db. Expected: {0}." \
                                                               " Actual: {1}".format(record_type, db_record_type)
        if 'dial_sched_time' in payload['data']['fields']:
            assert [dial_sched_time] == list(set(db_dial_sched_time)), \
                "Incorrect record_type found in db. Expected: {0}. Actual: {1}".format(dial_sched_time,
                                                                                       db_dial_sched_time)
        if 'campaign_id' in payload['data']['fields']:
            assert [campaign_id] == list(set(db_campaign_id)), "Incorrect record_type found in db. Expected: {0}." \
                                                               " Actual: {1}".format(campaign_id, db_campaign_id)
        if 'group_id' in payload['data']['fields']:
            assert [group_id] == list(set(db_group_id)), "Incorrect record_type found in db. Expected: {0}." \
                                                         " Actual: {1}".format(group_id, db_group_id)
        if 'agent_id' in payload['data']['fields']:
            assert [agent_id] == list(set(db_agent_id)), "Incorrect agent_id found in db. Expected: {0}." \
                                                         " Actual: {1}".format(agent_id, db_agent_id)
