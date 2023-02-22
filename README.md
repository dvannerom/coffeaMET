# coffeaMET

Running on data is done under analysis. Do:

python run_Run2.py -i data/EGamma2018A.json # or whatever input json file

This will create two output files (one .root and one .coffea) under results/year/, where "year" is the data taking year (be sure to have created that directory and to be saving under it). This folder can be changed in run_Run2.py.

Here are the steps to follow to submit condor jobs on lxplus (works for nanoAOD data stored on lxplus):

1. Generate a file containing all nanoAOD files to process. The file should contain one file per line, the location of which starting by /eos. Use quotes (e.g. "/eos/.../file.root"). Store the file in txt format under data, as for the example data/EGamma2018A_eos.txt.
2. Run makeJSON.py to split this file in json format readable by COFFEA. In makeJSON.py, change the input data file (the data/*_eos.txt) and the number of files per job. You will end up with a series of json files under data, each carrying an index.
3. Under condor/, open and edit the list of input json files in multisubmit.py to match the one just crated and run the script. It will create a series of condor submit_index.sub files and submit them.
4. After all jobs have run, merge the output root files and process them.
