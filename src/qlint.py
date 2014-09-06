#!/usr/bin/env python

NO_ERRORS_EXIT = 0
ERRORS_EXIT = 1
WARNINGS_EXIT = 2
CAN_NOT_OPEN_EVENT_FILE = 11
CAN_NOT_OPEN_PBS = 12
UNDEFINED_EVENT = 13

if __name__ == '__main__':
    from argparse import ArgumentParser
    import json, sys
    from vsc.qlint.pbs_parser import PbsParser

    arg_parser = ArgumentParser(description='PBS script syntax checker')
    arg_parser.add_argument('pbs_file', help='PBS file to check')
    arg_parser.add_argument('--events', default='events.json',
                            help='event defintion file to use')
    arg_parser.add_argument('--show_job', action='store_true',
                            help='show job parameters')
    arg_parser.add_argument('--quiet', action='store_true',
                            help='do not show summary')
    arg_parser.add_argument('--warnings_as_errors', action='store_true',
                            help='non zero exit code on warnings')
    options, rest = arg_parser.parse_known_args()
    try:
        with open(options.events) as event_file:
            event_defs = json.load(event_file)
    except EnvironmentError as error:
        msg = "### error: can not open event file '{0}'\n"
        sys.stderr.write(msg.format(options.events))
        sys.exit(CAN_NOT_OPEN_EVENT_FILE)
    pbs_parser = PbsParser()
    try:
        with open(options.pbs_file, 'r') as pbs_file:
            pbs_parser.parse_file(pbs_file)
    except EnvironmentError as error:
        msg = "### error: can not open PBS file '{0}'\n"
        sys.stderr.write(msg.format(options.events))
        sys.exit(CAN_NOT_OPEN_PBS)
    nr_warnings = 0
    nr_errors = 0
    for event in pbs_parser.events:
        eid = event['id']
        if eid in event_defs:
            msg_tmpl = event_defs[eid]['message']
            msg = msg_tmpl.format(**event['extra'])
            rem_tmpl = event_defs[eid]['remedy']
            rem = rem_tmpl.format(**event['extra'])
            if event_defs[eid]['category'] == 'error':
                cat = 'E'
                nr_errors += 1
            elif event_defs[eid]['category'] == 'warning':
                cat = 'W'
                nr_warnings += 1
            output_fmt = ('{cat} line {line:d}:\n'
                          '    problem: {msg}\n'
                          '    remedy:  {rem}')
            print output_fmt.format(cat=cat, line=event['line'],
                                    msg=msg, rem=rem)
        else:
            msg = "### internal error: unknown event id '{0}'\n"
            sys.stderr.write(msg.format(id))
            sys.exit(UNDEFINED_EVENT)
    if not options.quiet:
        print '{err:d} errors, {warn:d} warnings'.format(warn=nr_warnings,
                                                         err=nr_errors)
    if options.show_job:
        print pbs_parser.job.attrs_to_str()
    if nr_errors > 0:
        sys.exit(ERRORS_EXIT)
    elif options.warnings_as_errors and nr_warnings > 0:
        sys.exit(WARNINGS_EXIT)
    else:
        sys.exit(NO_ERRORS_EXIT)

