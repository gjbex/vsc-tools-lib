#!/usr/bin/env python

import subprocess
from vsc.moab.job import ShowqParser
from vsc.moab.checkjob import CheckjobParser

def get_blocked_jobs(options):
    '''Get a list of options currently in the queue''';
    cmd_ouput = subprocess.check_output([options.showq])
    parser = ShowqParser()
    jobs = parser.parse(cmd_ouput)
    return [job for job in jobs['blocked'] if job.state == 'SystemHold']

def get_hold_info(jobs, options):
    '''Get the information on the jobs that are on hold'''
    parser = CheckjobParser()
    for job in jobs:
        cmd_output = subprocess.check_output([options.checkjob, job.id])
        parser.parse(job, cmd_output)

if __name__ == '__main__':
    from argparse import ArgumentParser

    arg_parser = ArgumentParser(description='check systemhold jobs')
    arg_parser.add_argument('--showq', default='/opt/moab/bin/showq',
                            help='showq to use')
    arg_parser.add_argument('--checkjob', default='/opt/moab/bin/checkjob',
                            help='checkjob to use')
    options = arg_parser.parse_args()
    jobs = get_blocked_jobs(options)
    get_hold_info(jobs, options)
    for job in jobs:
        print job.id
        print '\t{0}'.format(job.username)
        print '\t{0}'.format(job.account)
        print '\t{0}'.format(job.holds)
