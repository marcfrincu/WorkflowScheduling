# WorkflowScheduling in Python

I worked on this project while doing my postdoc at University of Strasbourg between 2012-2013. The entire code is written in Python and allows users to simulate various workflow scheduling algorithms for cloud systems.

The particular setup used AWS and per hour billing (still in use at that time).

It allows the export of plots in *.plt* format for plotting nice graphs in PSTricks as well as *.arff* files containing the best scheduling algorithms which can be used in WEKA for machine learning.

The code also allows to export workflows to be used by the [SimSchlouder](https://gitlab.unistra.fr/gossa/schiaas-tutorial) simulator developed at the same time by a different team in the group.

To run the code see *experiments.py* file. Some basic configuration is explained in *config.py*.

Results were published in the [Computing journal (2014)](https://dl.acm.org/doi/10.1007/s00607-014-0410-0) and [IPDPS Workshops (2013)](https://ieeexplore.ieee.org/document/6651116).
