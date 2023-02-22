from os import system

jsons = ["/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_1.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_2.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_3.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_4.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_5.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_6.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_7.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_8.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_9.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_10.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_11.json",
         "/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/analysis/data/EGamma_2018A_12.json"
        ]

for json in jsons:
	index = json.split('/')[-1].split('.')[0].split('_')[-1]
	system("cp submit.sub submit_"+str(index)+".sub")
	json = json.replace("/","\/")
	json = json.replace(".","\.")
	json = json.replace("_","\_")
	system("sed -i 's/arguments    = json/arguments    = \""+json+"\"/' submit_"+str(index)+".sub")
	#system("condor_submit submit_"+str(index)+".sub")
