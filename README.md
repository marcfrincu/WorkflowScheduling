# WorkflowScheduling in Python

## Description

I worked on this project while doing my postdoc at University of Strasbourg between 2012-2013. The entire code is written in Python and allows users to simulate various workflow scheduling algorithms for cloud systems.

The particular setup used AWS and per hour billing (still in use at that time).

It allows the export of plots in *.plt* format for plotting nice graphs in PSTricks as well as *.arff* files containing the best scheduling algorithms which can be used in WEKA for machine learning.

The code also allows to export workflows to be used by the [SimSchlouder](https://gitlab.unistra.fr/gossa/schiaas-tutorial) simulator developed at the same time by a different team in the group.

To run the code see *experiments.py* file. Some basic configuration is explained in *config.py*.

## Published papers

Results were published in:

- M. Frincu, S. Genaud, J. Gossa,  [On the efficiency of several VM provisioning strategies for workflows with multi-threaded tasks on clouds](https://dl.acm.org/doi/10.1007/s00607-014-0410-0), Computing, vol 96(11), pp. 1059–1086, 2014.
- M. Frincu, S. Genaud, J. Gossa, [Comparing Provisioning and Scheduling Strategies for Workflows on Clouds](https://ieeexplore.ieee.org/document/6651116), Procs. IEEE IPDPS Workshops, 2013.
