#!/usr/bin/python -O
# -*- coding:utf8 -*-

"""
	This script take a JSON output from Schlouder as input, and output 
	a pstricks representing the different BTUs

"""

import sys
import os
import json


def usage():
	print >> sys.stderr, 'Usage: {0} json_file '.format(sys.argv[0])

# Date of the first vm start
beginDate = 0



# Layouts	
btuH = 0.2  # Height of btu/vm
btuM = 0.5    # Margin between 2 btu/vm
jobM = 0.0  # Up and down margin of jobs inside BTU

# Time to Point
def t2p(s):
	return (float(s)/60.0)
# Date to Point
def d2p(d):
	return t2p(d-beginDate)



if __name__ == '__main__':
	if len(sys.argv) != 2:
		usage()
		sys.exit(-1)

	# Open the JSON
	with open(sys.argv[1]) as fp:
		results = json.load(fp)
		print results['info']
		# If the json is a more recent version
		if 'nodes' in results:			
			results = results['nodes']
	results.sort(key=lambda vm:int(vm['start_date']))
	beginDate = int(results[0]['start_date'])


	# Output the file with the number of concurrent jobs
	output='wf-btu.tikz'
	if (os.path.isfile(output)):
		os.unlink(output)
	with open(output, 'w') as fp:
		# fp.write("\\begin{tikzpicture}")

#		for d in diametersJobs:
#			fp.write("{0}\t{1}\n".format(d[0]-beginDate, d[1]))

		iv = 0
		for vm in results:
			(start_date, predicted_boot_time, boot_time, stop_date) = (d2p(int(vm['start_date'])), t2p(int(vm['predicted_boot_time'])), t2p(int(vm['boot_time'])), d2p(int(vm['stop_date'])))

			if (stop_date == d2p(0)):
				stop_date = start_date+t2p(3600);


			fp.write("\n%%%%%%%%%%%%%%%%%%% VM {0}\n".format(iv))
			dY=iv*(btuH+btuM)

			#boottime
			fp.write("\\filldraw[draw=black,fill=lightgray,very thick] ({0},{1}) rectangle ({2},{3});\n"
			.format(start_date,dY,start_date+boot_time,dY+btuH))

			#btu
			fp.write("\\filldraw[draw=black,fill=white, very thick] ({0},{1}) rectangle ({2},{3});\n"
			.format(start_date+boot_time,dY,stop_date,dY+btuH))

			#jobs
			jobs = vm['jobs']			
			jobs.sort(key=lambda j: int(j['submission_date']), reverse=True)


			for job in jobs:
				(jreal_duration,jduration,jstart_date,jid,jsubmission_date) = (t2p(job['real_duration']), t2p(job['duration']), d2p(job['start_date']), job['id'], d2p(job['submission_date']))

				# real job
				fp.write("\\filldraw[draw=black,fill=cyan, very thin] ({0},{1}) rectangle ({2},{3});\n"
				.format(jstart_date,dY+jobM,jstart_date+jreal_duration,dY+btuH-jobM))
				
				fp.write("\\draw ({0},{1}) node "
				.format(jstart_date+jreal_duration/2,dY+jobM))
				fp.write("{Task "+str(jid)+"!}")
				fp.write(";\n")
				
			for job in jobs:
				(jreal_duration,jduration,jstart_date,jid,jsubmission_date) = (t2p(job['real_duration']), t2p(job['duration']), d2p(job['start_date']), job['id'], d2p(job['submission_date']))

				# sub date
				# nicer, but more limited 
				fp.write("\\draw[->,color=red,>=latex,very thin] ({0},{1}) -- ({4},{5}) .. controls ({6},{5}) .. ({2},{3});\n"
				.format(jsubmission_date,dY, jstart_date,dY+jobM, jsubmission_date,dY-btuM/2.0,(2*jstart_date+jsubmission_date)/3.0))



			#for job in jobs:
			#	(jreal_duration,jduration,jstart_date,jid,jsubmission_date) = (t2p(job['real_duration']), t2p(job['duration']), d2p(job['start_date']), job['id'], d2p(job['submission_date']))				

				# forecasted duration
			#	fp.write("\\filldraw[draw=black,fill=green,very thin] ({0},{1}) rectangle ({2},{3});\n"
			#	.format(jstart_date+jreal_duration,dY+jobM,jstart_date+jduration,dY+btuH/2.0))
			iv+=1

			fp.write("\\filldraw[draw=black,fill=yellow,very thin] ({0},{1}) rectangle ({2},{3});\n"
			.format(start_date+predicted_boot_time,dY,start_date+boot_time,dY+btuH/2))
		# fp.write("\\end{tikzpicture}")
	

