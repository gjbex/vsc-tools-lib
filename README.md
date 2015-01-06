qlint
=====
Sanity checker for PBS torque job files

Problems signalled
------------------
Errors:
  * Non-unix line endings
  * Non-ASCII charactersa in PBS file
  * Invalid values for join (-j)
  * Invalid values for keep (-k)
  * Invalid values for mail event (-m)
  * Malformed job name (-N)
  * Malformed project name (-A)
  * Invalid walltime format (-l walltime=...)
  * Non-numerical ppn, procs, or gpus specification
  * Invalid memory and process memory specification
  * Multpile procs resource specifications
  * Memory resources not available for combination of nodes, ppn, and pmem
  * Memory resources not available for combinatino of nodes and mem
  * Unknown partition specified
  * Unknown QOS specified
  * Number of nodes requested too large in partition
  * ppn too large

Warnings:
  * Missing shebang
  * Misplaced shebang
  * Space in directive, i.e., '# PBS', rather than '#PBS'
  * PBS directives after first script statement
  * Missing script
  * Invalid mail addresses (-M)
  * Unknown resource specication (-l)
  * Memory specification for pmem and mem not consistent

