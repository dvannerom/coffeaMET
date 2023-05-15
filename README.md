# coffeaMET

The framework is factorized into:

- The main running script run.py (run_Run2.py for Run2)
- The performance.py script that computes the recoil components and the response for uncorrected and corrected MET algorithms
- The processors for the considered final states under processors/ (currently only DY+jets and Gamma+jets)

To run the code (on lxplus), first source the setup script (you may have to change it a bit to your own environment):

source setup_lcg.sh

Then, generate a json file containing the files to run over. An example of such file can be found under data/.

Finally, just run:

python run.py -i input_json (--isData if you're running on data)

This will create two output files (one .root and one .coffea) under results/year/, where "year" is the data taking year (be sure to have created that directory and to be saving under it). This folder can be changed in run.py.

To run over large amount of data, you will have to use condor jobs:

1. Generate a file containing all nanoAOD files to process. The file should contain one file per line, the location of which starting by /eos. Use quotes (e.g. "/eos/.../file.root"). Store the file in txt format under data, as for the example data/EGamma2018A_eos.txt.
2. Run makeJSON.py to split this file in json format readable by COFFEA. In makeJSON.py, change the input data file (the data/*_eos.txt) and the number of files per job. You will end up with a series of json files under data, each carrying an index.
3. Under condor/, open and edit the list of input json files in multisubmit.py to match the one just crated and run the script. It will create a series of condor submit_index.sub files and submit them.
4. After all jobs have run, merge the output root files and process them.

