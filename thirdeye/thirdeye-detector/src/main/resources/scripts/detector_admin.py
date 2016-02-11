# author: jteoh
# note: This is a snapshot of a script in my utils repo: https://gitli.corp.linkedin.com/jteoh/utils/source/master:thirdeye/detector_admin.py
# Use that url for the latest version of the script.

# fix desktop python path for argparse
import sys
sys.path.insert(1, '/usr/local/linkedin/lib/python2.6/site-packages')

import argparse
import cmd
from datetime import date, datetime, timedelta
import json
from pprint import pprint
import httplib
import re
import urllib


client = None


class ThirdEyeHttpClient(object):
    def __init__(self, base, app_port=19044, admin_port=11120):
        base = str(base)
        print "Using host: ", base
        self.application_host = base + ":" + str(app_port)
        self.admin_host = base + ":" + str(admin_port)

    def curl(self, method, endpoint, additional_params={}):
        return self.curl_helper(method, endpoint, **additional_params)

    def curl_helper(self, method, endpoint, data=None, print_result=False, is_admin_request=False):
        host = self.application_host if not is_admin_request else self.admin_host
        print method, host + endpoint, data or ''
        conn = httplib.HTTPConnection(host)
        conn.request(method, endpoint, data, headers={'Content-type': 'application/json'})
        resp = conn.getresponse()
        result = resp.read()
        conn.close()
        status = resp.status
        reason = resp.reason
        print status, reason
        if status == 200 and result:
            #byteify if applicable
            try:
                result = byteify(json.loads(result))
            except Exception:
                pass

        if print_result:
            if status == 200 or 204:  # 204 = no content
                if callable(print_result):
                    result = print_result(result)
                elif not result and type(print_result) == str:
                    result = print_result
            if result:
                if type(result) == str:
                    print result
                else:
                    pprint(result)
        #TODO raise error if failed.
        return resp.status, resp.reason, result

FUNCTIONS_ENDPOINT = '/api/anomaly-functions/'
JOBS_ENDPOINT = '/api/anomaly-jobs/'
EMAIL_REPORTS_ENDPOINT = '/api/email-reports/'
API = '/api/'
FUNCTIONS_ENDPOINT = API + 'anomaly-functions/'
JOBS_ENDPOINT = API + 'anomaly-jobs/'
EMAIL_REPORTS_ENDPOINT = API + 'email-reports/'
ANOMALY_RESULTS_ENDPOINT = API + 'anomaly-results/'

MULTIPLE_INP_KEY = "inps"

""" Command Loop """


class DetectorAdminShell(cmd.Cmd):
    intro = "Type ? or 'help' for a full list of available command line commands, or 'usage' for detector actions."
    prompt = "\n(thirdeye-detector) "

    def __init__(self, parser):
        self.parser = parser
        cmd.Cmd.__init__(self)

    def default(self, line):
        try:
            args = vars(self.parser.parse_args(line.split()))
            func = args.pop('func')
            func(**args)
        except SystemExit:
            #keep looping if the internal parser tries to exit.
            pass
        except Exception as e:
            print type(e), e

    def do_bye(self, arg):
        #DUBAI hehe :D
        'Exits in a fun manner.'
        return self._exit_()

    def do_exit(self, arg):
        'Exits the current program.'
        return self._exit_()

    def do_quit(self, arg):
        'Exits the current program.'
        return self._exit_()

    def do_usage(self, arg):
        'Displays usage info detector admin commands'
        self.parser.print_help()

    def help_help(self):
        #really??
        print "Really? Shows a help message"

    def start(self):
        try:
            self.cmdloop()
        except KeyboardInterrupt:
            self._exit_()

    def _exit_(self):
        print "Exiting..."
        return True

""" Parsers """


def add_function_subparser(subparsers):
    """ GET, GET <id>, POST <data>, DELETE <id> """
    functions = subparsers.add_parser('functions', help='anomaly function definitions')
    function_subparsers = functions.add_subparsers()

    show_parser = function_subparsers.add_parser('show', help='show all functions')
    show_parser.set_defaults(func=show_functions)

    show_ids_parser = function_subparsers.add_parser('show_ids', help='show only function ids')
    show_ids_parser.set_defaults(func=show_function_ids)

    find_parser = function_subparsers.add_parser('find', help='find a function')
    find_parser.add_argument('inps', type=int, nargs='+', help='function ids', metavar='ids')
    find_parser.set_defaults(func=find_function)

    create_parser = function_subparsers.add_parser('create', help='create a new function')
    create_parser.add_argument('inps', nargs='+', help='JSON files specifying functions to be created', metavar='file_paths')
    create_parser.set_defaults(func=create_function)

    delete_parser = function_subparsers.add_parser('delete', help='delete a function')
    delete_parser.add_argument('inps', type=int, nargs='+', help='function ids', metavar='ids')
    delete_parser.set_defaults(func=delete_function)


def add_jobs_subparser(subparsers):
    """ GET, POST <id>, POST <id> (adhoc, optional start+end), DELETE <id> """
    jobs = subparsers.add_parser('jobs', help='anomaly function schedules')
    jobs_subparsers = jobs.add_subparsers()

    show_parser = jobs_subparsers.add_parser('show', help='show all active jobs')
    show_parser.set_defaults(func=show_active_jobs)

    enable_parser = jobs_subparsers.add_parser('enable', help='enable job schedule')
    enable_parser.add_argument('inps', type=int, nargs='+', help='job ids', metavar='ids')
    enable_parser.set_defaults(func=enable_job)

    adhoc_parser = jobs_subparsers.add_parser('adhoc', help='run adhoc job')
    adhoc_parser.add_argument('inps', type=int, nargs='+', help='job ids', metavar='ids')
    adhoc_parser.add_argument('--start', help='start time in IS08601 or as daysago(#)', required=False)
    adhoc_parser.add_argument('--end', help='end time in IS08601 or as daysago(#)', required=False)
    adhoc_parser.set_defaults(func=adhoc_job)

    disable_parser = jobs_subparsers.add_parser('disable', help='disable job schedule')
    disable_parser.add_argument('inps', type=int, nargs='+', help='job ids', metavar='ids')
    disable_parser.set_defaults(func=disable_job)


def add_email_reports_subparser(subparsers):
    """ GET, GET <id>, POST <data>, POST <id> (adhoc), DELETE <id> """
    email_reports = subparsers.add_parser('reports', help='email report definitions')
    email_reports_subparser = email_reports.add_subparsers()

    show_parser = email_reports_subparser.add_parser('show', help='show all email reports')
    show_parser.set_defaults(func=show_email_reports)

    find_parser = email_reports_subparser.add_parser('find', help='find an email report')
    find_parser.add_argument('inps', type=int, nargs='+', help='email_report ids', metavar='ids')
    find_parser.set_defaults(func=find_email_report)

    create_parser = email_reports_subparser.add_parser('create', help='create a new email report. be sure to reset the scheduler afterwards!')
    create_parser.add_argument('inps', nargs='+', help='JSON files specifying email reports to be created', metavar='file_paths')
    create_parser.set_defaults(func=create_email_report)

    adhoc_parser = email_reports_subparser.add_parser('adhoc', help='send adhoc email report')
    adhoc_parser.add_argument('inps', type=int, nargs='+', help='email_report_ids', metavar='ids')
    adhoc_parser.set_defaults(func=adhoc_email_report)

    delete_parser = email_reports_subparser.add_parser('delete', help='delete an email report')
    delete_parser.add_argument('inps', type=int, nargs='+', help='email_report ids', metavar='ids')
    delete_parser.set_defaults(func=delete_email_report)

    reset_parser = email_reports_subparser.add_parser('reset', help='reset the email scheduler, required for changes to take effect')
    reset_parser.set_defaults(func=reset_email_scheduler)


def add_anomaly_results_subparser(subparsers):
    """ GET <id>, GET <collection> <start> [<end>], POST <data>, DELETE <id> """
    # Would be nice to have:
    # 1. Find by function id
    # 2. Show all
    results = subparsers.add_parser('results', help='anomaly results')
    results_subparser = results.add_subparsers()

    find_parser = results_subparser.add_parser('find', help='find an anomaly result')
    find_parser.add_argument('inps', type=int, nargs='+', help='result ids', metavar='ids')
    find_parser.set_defaults(func=find_anomaly_result)

    show_parser = results_subparser.add_parser('show', help='show anomaly results for a collection + time frame')
    show_parser.add_argument('collection', help='thirdeye collection')
    show_parser.add_argument('--start', help='start time in IS08601 or as daysago(#), default=daysago(7)', required=False, default=convert_to_iso('daysago(7)'))
    show_parser.add_argument('--end', help='end time in IS08601 or as daysago(#)', required=False)
    show_parser.set_defaults(func=show_anomaly_results_for_collection)

    # create_parser = results_subparser.add_parser('create', help='create a new anomaly result')
    # create_parser.add_argument('inps', nargs='+', help='JSON files specifying result to be created', metavar='file_paths')
    # create_parser.set_defaults(func=create_anomaly_result)

    # delete_parser = results_subparser.add_parser('delete', help='delete an anomaly result')
    # delete_parser.add_argument('inps', type=int, nargs='+', help='result ids', metavar='ids')
    # delete_parser.set_defaults(func=delete_anomaly_result)

""" Utility methods """


# Remove unicode encoding: http://stackoverflow.com/questions/956867/how-to-get-string-objects-instead-of-unicode-ones-from-json-in-python
def byteify(input):
    if isinstance(input, dict):
        return dict([(byteify(key), byteify(value)) for key, value in input.iteritems()])
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input


def action_msg_generator(entity, action):
    return lambda s: str(action).capitalize() + ' ' + str(entity) + ': ' + str(s)


def delete_msg_success(entity):
    return action_msg_generator(entity, 'deleted')


def create_msg_success(entity):
    return action_msg_generator(entity, 'created')


def convert_to_iso(s):
    result = None
    m = re.search('daysago\((\d+)\)', s)
    if m:
        daysago = -int(m.group(1))
        today = datetime.combine(datetime.now(), datetime.min.time())
        target_date = today - timedelta(days=(-1 * daysago))
        result = target_date.isoformat()
    else:
        #TODO don't simply assume date is iso 8601 compatible...
        result = s
    return result


""" Decorators for sending requests """


# Credit for guidance from http://thecodeship.com/patterns/guide-to-python-function-decorators/
def Request(func):
    def func_wrapper(*args, **kwargs):
        curl_params = func(*args, **kwargs)
        status, reason, result = client.curl(*curl_params)
        return status, reason, result
    return func_wrapper


def MultipleInps(func):
    def func_wrapper(inps, **args):
        results = []
        failed = []
        for inp in inps:
            try:
                if args:
                    result = func(inp, **args)
                else:
                    result = func(inp)
                results.append(result)
            except Exception as e:
                failed.append(inp)
                print e
        if failed:
            print "Failed: ", failed
        return results
    return func_wrapper

""" Actual parser methods """


@Request
def show_functions(print_result=True):
    print "Retrieving functions"
    return 'GET', FUNCTIONS_ENDPOINT, {'print_result': print_result}


def show_function_ids():
    status, reason, functions = show_functions(print_result=False)
    pprint([f['id'] for f in functions])


@MultipleInps
@Request
def find_function(id):
    print "Finding function id %d" % id
    return 'GET', FUNCTIONS_ENDPOINT + str(id), {'print_result': True}


@MultipleInps
@Request
def create_function(file_path):
    print "Creating function from file_path %s" % file_path
    #callable
    with open(file_path, 'r') as f:
        data = f.read()
    return 'POST', FUNCTIONS_ENDPOINT, {'data': data, 'print_result': create_msg_success('function')}


@MultipleInps
@Request
def delete_function(id):
    print "Deleting function id %d" % id
    return 'DELETE', FUNCTIONS_ENDPOINT + str(id), {'print_result': "Deleted function id %d" % id}


@Request
def show_active_jobs():
    print "Showing active jobs"
    return 'GET', JOBS_ENDPOINT, {'print_result': True}


@MultipleInps
@Request
def enable_job(id):
    print "Enabling job id %d" % id
    return 'POST', JOBS_ENDPOINT + str(id)


@MultipleInps
@Request
def adhoc_job(id, start=None, end=None):
    if bool(start) != bool(end):
        raise ValueError("Both start and end are required if either is present")
    if start and end:
        start = convert_to_iso(start)
        end = convert_to_iso(end)
        print "Running adhoc job id %d on window %s to %s" % (id, start, end)
        return 'POST', JOBS_ENDPOINT + str(id) + '/ad-hoc?' + urllib.urlencode({'start': start, 'end': end})
    else:
        print "Running adhoc job id %d" % id
        return 'POST', JOBS_ENDPOINT + str(id) + '/ad-hoc'


@MultipleInps
@Request
def disable_job(id):
    print "Disabling job id %d" % id
    return 'DELETE', JOBS_ENDPOINT + str(id)


@Request
def show_email_reports():
    print "Showing email_reports"
    return 'GET', EMAIL_REPORTS_ENDPOINT, {'print_result': True}


@MultipleInps
@Request
def find_email_report(id):
    print "Finding email report id %d" % id
    return 'GET', EMAIL_REPORTS_ENDPOINT + str(id), {'print_result': True}


@MultipleInps
@Request
def create_email_report(file_path):
    print "Creating email report from file_path %s" % file_path
    with open(file_path, 'r') as f:
        data = f.read()
    return 'POST', EMAIL_REPORTS_ENDPOINT, {'data': data, 'print_result': create_email_report_helper}


def create_email_report_helper(result):
    print create_msg_success('email report')(result), "\n"
    reset_now = raw_input("The email scheduler must be reset for changes to take place. Do you want to reset the scheduler right now (Y/n): ")
    if reset_now is "Y":
        reset_email_scheduler()
    return None


@MultipleInps
@Request
def adhoc_email_report(id):
    print "Running adhoc email report id %d" % id
    return 'POST', EMAIL_REPORTS_ENDPOINT + str(id) + '/ad-hoc'


@MultipleInps
@Request
def delete_email_report(id):
    print "Deleting email report id %d" % id
    return 'DELETE', EMAIL_REPORTS_ENDPOINT + str(id), {'print_result': "Deleted email report id %d" % id}


#Special instance that needs to hit the admin port
def reset_email_scheduler():
    print "Resetting email scheduler"
    status, reason, result = client.curl('POST', '/admin/tasks/email?action=reset', {'is_admin_request': True})


@MultipleInps
@Request
def find_anomaly_result(id):
    print "Finding anomaly result id %d" % id
    return 'GET', ANOMALY_RESULTS_ENDPOINT + str(id), {'print_result': True}


@Request
def show_anomaly_results_for_collection(collection, start, end=None):
    start = convert_to_iso(start)
    url = ANOMALY_RESULTS_ENDPOINT + str(collection) + '/' + start
    if end:
        end = convert_to_iso(end)
        url += '/' + end
    print url
    return 'GET', url, {'print_result': True}

""" Initialization code """
hosts = ["localhost", "lva1-app0430", "lva1-app0418", "jteoh-ld1"]


def extract_host():
    if len(sys.argv) > 1:
        host = sys.argv[1]
    else:
        host = hosts[0]
    if host not in hosts:
        print 'WARNING: host "', host, '"is not officially supported'
    return host


def main():
    host = extract_host()
    global client
    client = ThirdEyeHttpClient(host)

    parser = argparse.ArgumentParser(description='Python REST Client for Anomaly Detection Server endpoints')
    subparsers = parser.add_subparsers()
    add_function_subparser(subparsers)
    add_jobs_subparser(subparsers)
    add_email_reports_subparser(subparsers)
    add_anomaly_results_subparser(subparsers)

    shell = DetectorAdminShell(parser)
    shell.start()

if __name__ == '__main__':
    main()
