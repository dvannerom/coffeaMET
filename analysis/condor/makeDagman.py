from os.path import exists

dagFile = "condor_jobs/submit_2018A.dag"

data = "EGamma2018A"

i = 1
loop = True
while loop:
	if exists("data/EGamma_2018A_"+str(i)+".json"):
		if not exists("results/2018/"+data+"_"+str(i)+".root"):
			with open(dagFile, "a") as f:
				f.write("JOB  EG2018A_"+str(i)+" /afs/cern.ch/user/v/vannerom/work/MET/decafMET/decafMET/analysis/condor_jobs/submit.condor\n")
				f.write("VARS EG2018A_"+str(i)+" jsonFile=\"/afs/cern.ch/user/v/vannerom/work/MET/decafMET/decafMET/analysis/data/EGamma_2018A_"+str(i)+".json\"\n")
				f.write("RETRY all_nodes 2\n")
		i += 1
	else: loop = False
