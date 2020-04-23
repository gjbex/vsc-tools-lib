# vsc-tools-lib

Library of tools to parse output of PBS torque and Adaptive Moab tools,
and represent the relevant data.

## Functionality for PBS torque

* `vsc.pbs.job`: representation of a PBS job
* `vsc.pbs.qstat`: parser for output of the `qstat -f` command
* `vsc.pbs.node`: representation of a PBS node
* `vsc.pbs.job_analysis`: convenience class for PBS log analysis
* `vsc.pbs.job_event`: representation of job events in PBS log files
* `vsc.pbs.log`: PBS torque log file parser
* `vsc.pbs.pbsnodes`: parser for the output of the `pbsnodes` command
* `vsc.pbs.script_parser`: parser for PBS script files
* `vsc.pbs.option_parser`: parser for PBS options, used by
    `vsc.pbs.script_parser`
* `vsc.pbs.utils`: auxiliary functions (site specific)
* `vsc.pbs.check`: semantic checks of a PBS job specification


## Functionality for Adaptive Moab

* `vsc.moab.job`: representation of a PBS job's status
* `vsc.moab.showq`: parser for the output of the `showq` command
* `vsc.moab.checkjob`: parser for a small part of the output of the
    `checkjob` command


## Functionality for Adaptive MAM

* `vsc.mam.account`: representation of a MAM account
* `vsc.mam.gbalance`: parser for the output of the `gbalance` command


## Utilities

* `vsc.eventlogger`: base class that acts as a logger for errors and
    warnings, `vsc.pbs.script_parser` and `vsc.pbs.option_parser` extend
    it
* `vsc.utils`: functions for time and size conversion
* `vsc.plotly`: functions to annotate plotly graphs


## Dependencies

* Python 3.5+
* validate_email
* fuzzywuzzy


## Reverse dependencies
* https://github.com/gjbex/qlint
* https://github.com/gjbex/vsc-cluster-db
* https://github.com/gjbex/vsc-monitoring
