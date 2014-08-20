qlint
=====
Sanity checker for PBS torque job files

Problems signalled
------------------
Errors:
  * Non-unix line endings
  * Non-ASCII charactersa
  * Invalid values for join (-j)
  * Invalid values for mail event (-m)
  * Malformed job name (-N)

Warnings:
  * Missing shebang
  * Misplaced shebang
  * Space in directive, i.e., '# PBS', rather than '#PBS'
  * PBS directives after first script statement
  * Missing script

