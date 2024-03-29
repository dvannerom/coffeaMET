import os, sys

import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea import processor, hist, util
from coffea.nanoevents.methods import vector
from coffea.nanoevents.methods.vector import ThreeVector

import uproot3
import hist2root
import ROOT
from ROOT import TFile

import array
import json
import numpy as np
import argparse

import time

import performance_Run2

def hname():
	import socket
	return socket.gethostname()

def read_json_file(filename):
    with open(filename, 'r') as f:
        cache = f.read()
        data = eval(cache)
    return data

if __name__ == '__main__':

	# Define input arguments
	parser = argparse.ArgumentParser(description='')
	parser.add_argument('-i', '--inFile', type=str, default='', help='Input json file')
	args = parser.parse_args()
	
	inFile = args.inFile

	index = inFile.split('/')[-1].split('.')[0].split('_')[-1]
	
	with open(inFile) as json_file:
		tic = time.time()
	
		data = json.load(json_file)
		for a in data.keys():
			print(a)
			fileset = {a: data[a]}
			exe_args = {
				"skipbadfiles": True,
				"schema": NanoAODSchema,
				"workers": 8,
				"savemetrics": True,
			}
			result, metrics = processor.run_uproot_job(
				fileset,
				"Events",
				processor_instance=performance_Run2.performanceProcessor(),
				#executor=processor.iterative_executor,
				executor=processor.futures_executor,
				executor_args=exe_args,
			)
			util.save(result,'results/2018/'+a+'_'+str(index)+'.coffea')
			output_root_file = 'results/2018/'+a+'_'+str(index)+'.root'
	
			elapsed = time.time() - tic
			print(f"Output: {result}")
			print(f"Metrics: {metrics}")
			print(f"Finished in {elapsed:.1f}s")
			print(f"Events/s: {metrics['entries'] / elapsed:.0f}")
			
			outputfile = uproot3.recreate(output_root_file)
			for var in result.keys():
				if var == 'histo1':
					outputfile['met_pf_raw_phi'] = hist.export1d(result[var].sum('dataset','met_pf_phi','met_puppi_raw_phi','met_puppi_phi'))
					outputfile['met_pf_phi'] = hist.export1d(result[var].sum('dataset','met_pf_raw_phi','met_puppi_raw_phi','met_puppi_phi'))
					outputfile['met_puppi_raw_phi'] = hist.export1d(result[var].sum('dataset','met_pf_raw_phi','met_pf_phi','met_puppi_raw_phi'))
					outputfile['met_puppi_phi'] = hist.export1d(result[var].sum('dataset','met_pf_raw_phi','met_pf_phi','met_puppi_phi'))
				elif var == 'histo2':
					outputfile['qt'] = hist.export1d(result[var].sum('dataset','pv','response_pf_raw','met_pf_raw'))
					outputfile['pv'] = hist.export1d(result[var].sum('dataset','qt','response_pf_raw','met_pf_raw'))
					outputfile['met_pf_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','response_pf_raw'))
					h_response_pf_raw = ROOT.TH1F()
					h_response_pf_raw.StatOverflows(ROOT.kTRUE)
					h_response_pf_raw = hist.export1d(result[var].sum('dataset','qt','pv','met_pf_raw',overflow='all'))
					outputfile['response_pf_raw'] = h_response_pf_raw
					#outputfile['response_pf_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','met_pf_raw'))
				elif var == 'histo3':
					outputfile['met_puppi_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','response_puppi_raw'))
					outputfile['response_puppi_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','met_puppi_raw'))
				elif var == 'histo4':
					outputfile['met_pf'] = hist.export1d(result[var].sum('dataset','qt','pv','response_pf'))
					outputfile['response_pf'] = hist.export1d(result[var].sum('dataset','qt','pv','met_pf'))
				elif var == 'histo5':
					outputfile['met_puppi'] = hist.export1d(result[var].sum('dataset','qt','pv','response_puppi'))
					outputfile['response_puppi'] = hist.export1d(result[var].sum('dataset','qt','pv','met_puppi'))
				elif var == 'histo6':
					outputfile['upar_pf_raw_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_puppi_raw_plus_qt'))
					outputfile['upar_puppi_raw_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_pf_raw_plus_qt'))
				elif var == 'histo7':
					outputfile['upar_pf_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_puppi_plus_qt'))
					outputfile['upar_puppi_plus_qt'] = hist.export1d(result[var].sum('dataset','qt','pv','upar_pf_plus_qt'))
				elif var == 'histo8':
					outputfile['uperp_pf_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_puppi_raw'))
					outputfile['uperp_puppi_raw'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_pf_raw'))
				elif var == 'histo9':
					outputfile['uperp_pf'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_puppi'))
					outputfile['uperp_puppi'] = hist.export1d(result[var].sum('dataset','qt','pv','uperp_pf'))
				elif var == 'histo10':
					outputfile['nEventsTot'] = hist.export1d(result[var])
				elif var == 'histo11':
					outputfile['nEventsLumi'] = hist.export1d(result[var])
				elif var == 'histo12':
					outputfile['nEventsFilters'] = hist.export1d(result[var])
				elif var == 'histo13':
					outputfile['nEventsTriggers'] = hist.export1d(result[var])
				elif var == 'histo14':
					outputfile['nEventsNPV'] = hist.export1d(result[var])
				elif var == 'histo15':
					outputfile['nEventsMuon'] = hist.export1d(result[var])
				#elif var == 'histo15':
				#	outputfile['nEventsPhoton'] = hist.export1d(result[var])
				#elif var == 'histo16':
				#	outputfile['nEventsLepton'] = hist.export1d(result[var])
				#elif var == 'histo17':
				#	outputfile['nEventsJet'] = hist.export1d(result[var])
				#elif var == 'histo18':
				#	outputfile['nEventsDeltaR'] = hist.export1d(result[var])
			outputfile.close()
			
			infile = TFile.Open(output_root_file,"update")
			infile.cd()
	
			# Define 2D histos
			h_2D_response_pf_raw_vs_qt = hist2root.convert(result['histo2'].sum('dataset','pv','met_pf_raw'))
			h_2D_response_pf_vs_qt = hist2root.convert(result['histo4'].sum('dataset','pv','met_pf'))
			h_2D_response_puppi_raw_vs_qt = hist2root.convert(result['histo3'].sum('dataset','pv','met_puppi_raw'))
			h_2D_response_puppi_vs_qt = hist2root.convert(result['histo5'].sum('dataset','pv','met_puppi'))
	
			h_2D_response_pf_raw_vs_pv = hist2root.convert(result['histo2'].sum('dataset','qt','met_pf_raw'))
			h_2D_response_pf_vs_pv = hist2root.convert(result['histo4'].sum('dataset','qt','met_pf'))
			h_2D_response_puppi_raw_vs_pv = hist2root.convert(result['histo3'].sum('dataset','qt','met_puppi_raw'))
			h_2D_response_puppi_vs_pv = hist2root.convert(result['histo5'].sum('dataset','qt','met_puppi'))
	
			h_2D_upar_pf_raw_vs_qt = hist2root.convert(result['histo6'].sum('dataset','pv','upar_puppi_raw_plus_qt'))
			h_2D_upar_pf_vs_qt = hist2root.convert(result['histo7'].sum('dataset','pv','upar_puppi_plus_qt'))
			h_2D_upar_puppi_raw_vs_qt = hist2root.convert(result['histo6'].sum('dataset','pv','upar_pf_raw_plus_qt'))
			h_2D_upar_puppi_vs_qt = hist2root.convert(result['histo7'].sum('dataset','pv','upar_pf_plus_qt'))
	
			h_2D_upar_pf_raw_vs_pv = hist2root.convert(result['histo6'].sum('dataset','qt','upar_puppi_raw_plus_qt'))
			h_2D_upar_pf_vs_pv = hist2root.convert(result['histo7'].sum('dataset','qt','upar_puppi_plus_qt'))
			h_2D_upar_puppi_raw_vs_pv = hist2root.convert(result['histo6'].sum('dataset','qt','upar_pf_raw_plus_qt'))
			h_2D_upar_puppi_vs_pv = hist2root.convert(result['histo7'].sum('dataset','qt','upar_pf_plus_qt'))
	
			h_2D_uperp_pf_raw_vs_qt = hist2root.convert(result['histo8'].sum('dataset','pv','uperp_puppi_raw'))
			h_2D_uperp_pf_vs_qt = hist2root.convert(result['histo9'].sum('dataset','pv','uperp_puppi'))
			h_2D_uperp_puppi_raw_vs_qt = hist2root.convert(result['histo8'].sum('dataset','pv','uperp_pf_raw'))
			h_2D_uperp_puppi_vs_qt = hist2root.convert(result['histo9'].sum('dataset','pv','uperp_pf'))
	
			h_2D_uperp_pf_raw_vs_pv = hist2root.convert(result['histo8'].sum('dataset','qt','uperp_puppi_raw'))
			h_2D_uperp_pf_vs_pv = hist2root.convert(result['histo9'].sum('dataset','qt','uperp_puppi'))
			h_2D_uperp_puppi_raw_vs_pv = hist2root.convert(result['histo8'].sum('dataset','qt','uperp_pf_raw'))
			h_2D_uperp_puppi_vs_pv = hist2root.convert(result['histo9'].sum('dataset','qt','uperp_pf'))
	
			# Write histos to file
			h_2D_response_pf_raw_vs_qt.Write("response_pf_raw_vs_qt")
			h_2D_response_pf_vs_qt.Write("response_pf_vs_qt")
			h_2D_response_puppi_raw_vs_qt.Write("response_puppi_raw_vs_qt")
			h_2D_response_puppi_vs_qt.Write("response_puppi_vs_qt")
			h_2D_response_pf_raw_vs_pv.Write("response_pf_raw_vs_pv")
			h_2D_response_pf_vs_pv.Write("response_pf_vs_pv")
			h_2D_response_puppi_raw_vs_pv.Write("response_puppi_raw_vs_pv")
			h_2D_response_puppi_vs_pv.Write("response_puppi_vs_pv")
			h_2D_upar_pf_raw_vs_qt.Write("upar_pf_raw_vs_qt")
			h_2D_upar_pf_vs_qt.Write("upar_pf_vs_qt")
			h_2D_upar_puppi_raw_vs_qt.Write("upar_puppi_raw_vs_qt")
			h_2D_upar_puppi_vs_qt.Write("upar_puppi_vs_qt")
			h_2D_upar_pf_raw_vs_pv.Write("upar_pf_raw_vs_pv")
			h_2D_upar_pf_vs_pv.Write("upar_pf_vs_pv")
			h_2D_upar_puppi_raw_vs_pv.Write("upar_puppi_raw_vs_pv")
			h_2D_upar_puppi_vs_pv.Write("upar_puppi_vs_pv")
			h_2D_uperp_pf_raw_vs_qt.Write("uperp_pf_raw_vs_qt")
			h_2D_uperp_pf_vs_qt.Write("uperp_pf_vs_qt")
			h_2D_uperp_puppi_raw_vs_qt.Write("uperp_puppi_raw_vs_qt")
			h_2D_uperp_puppi_vs_qt.Write("uperp_puppi_vs_qt")
			h_2D_uperp_pf_raw_vs_pv.Write("uperp_pf_raw_vs_pv")
			h_2D_uperp_pf_vs_pv.Write("uperp_pf_vs_pv")
			h_2D_uperp_puppi_raw_vs_pv.Write("uperp_puppi_raw_vs_pv")
			h_2D_uperp_puppi_vs_pv.Write("uperp_puppi_vs_pv")
	
			infile.Close()
