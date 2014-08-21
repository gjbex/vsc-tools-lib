#!/usr/bin/env python

if __name__ == '__main__':
    from argparse import ArgumentParser, FileType
    import json, sys
    from vsc.qlint.pbs_parser import PbsParser

    arg_parser = ArgumentParser(description='PBS script syntax checker')
    arg_parser.add_argument('pbs_file', help='PBS file to check')
    arg_parser.add_argument('--events', default='events.json',
                            help='event defintion file to use')
    options, rest = arg_parser.parse_known_args()
    with open(options.events) as event_file:
        event_defs = json.load(event_file)
    pbs_parser = PbsParser()
    pbs_parser.parse(options.pbs_file)
    for event in pbs_parser.events:
        id = event['id']
        if id in event_defs:
            msg_tmpl = event_defs[id]['message']
            msg = msg_tmpl.format(**event['extra'])
            rem_tmpl = event_defs[id]['remedy']
            rem = rem_tmpl.format(**event['extra'])
            if event_defs[id]['category'] == 'error':
                cat = 'E'
            elif event_defs[id]['category'] == 'warning':
                cat = 'W'
            output_fmt = ('{cat} line {line:d}:\n'
                          '    problem: {msg}\n'
                          '    remedy:  {rem}')
            print output_fmt.format(cat=cat, line=event['line'],
                                    msg=msg, rem=rem)
        else:
            msg = "### internal error: unknown event id '{o}'\n"
            sys.stderr.write(msg.format(id))
