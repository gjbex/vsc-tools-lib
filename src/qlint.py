#!/usr/bin/env python

CAN_NOT_OPEN_EVENT_FILE = 1
CAN_NOT_OPEN_PBS = 2
UNDEFINED_EVENT = 3

if __name__ == '__main__':
    from argparse import ArgumentParser
    import json, sys
    from vsc.qlint.pbs_parser import PbsParser

    arg_parser = ArgumentParser(description='PBS script syntax checker')
    arg_parser.add_argument('pbs_file', help='PBS file to check')
    arg_parser.add_argument('--events', default='events.json',
                            help='event defintion file to use')
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
    for event in pbs_parser.events:
        eid = event['id']
        if id in event_defs:
            msg_tmpl = event_defs[eid]['message']
            msg = msg_tmpl.format(**event['extra'])
            rem_tmpl = event_defs[eid]['remedy']
            rem = rem_tmpl.format(**event['extra'])
            if event_defs[eid]['category'] == 'error':
                cat = 'E'
            elif event_defs[id]['category'] == 'warning':
                cat = 'W'
            output_fmt = ('{cat} line {line:d}:\n'
                          '    problem: {msg}\n'
                          '    remedy:  {rem}')
            print output_fmt.format(cat=cat, line=event['line'],
                                    msg=msg, rem=rem)
        else:
            msg = "### internal error: unknown event id '{0}'\n"
            sys.stderr.write(msg.format(id))
            sys.exit(UNDEFINED_EVENT)
