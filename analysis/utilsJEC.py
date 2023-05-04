import os, sys

import awkward as ak
from coffea.jetmet_tools import FactorizedJetCorrector, JetCorrectionUncertainty
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory
from coffea.lookup_tools import extractor

import array
import json
import numpy as np

def isGoodJet(jet):
	pt = jet['pt']
	eta = jet['eta']
	jet_id = jet['jetId']
	nhf = jet['neHEF']
	chf = jet['chHEF']
	nef = jet['neEmEF']
	cef = jet['chEmEF']
	muf = jet['muEF']
        #mask = (pt>15) & ((jet_id&2)==2) & (nhf<0.8) & (chf>0.1) & (nef<0.9) & (cef<0.9)
	mask = (pt>8) & (eta > -5.2) & (eta < 5.2) & (cef < 0.8) & (muf < 0.8)
	return mask

def correctedDataPuppiJets(events,jets):
	ext = extractor()
	ext.add_weight_sets([
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi.txt"
	])
	
	ext.finalize()
	
	jec_stack_names = [
	        "Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi",
	        "Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi",
	        "Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi",
	        "Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi"
	]
	
	evaluator = ext.make_evaluator()
	
	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
	jec_stack = JECStack(jec_inputs)
	
	name_map = jec_stack.blank_name_map
	name_map['JetPt'] = 'pt'
	name_map['JetMass'] = 'mass'
	name_map['JetEta'] = 'eta'
	name_map['JetA'] = 'area'
	
	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
	#jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
	#name_map['ptGenJet'] = 'pt_gen'
	name_map['ptRaw'] = 'pt_raw'
	name_map['massRaw'] = 'mass_raw'
	name_map['Rho'] = 'rho'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
	        Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi'],
	        Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi'],
	        Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi'],
	        Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi']
	)
	
	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
	
	return corrected_jets

def correctedDataChsJets(events,jets):
	ext = extractor()
	ext.add_weight_sets([
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs.txt"
	])
	
	ext.finalize()
	
	jec_stack_names = [
	        "Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs",
	        "Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs",
	        "Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs",
	        "Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs"
	]
	
	evaluator = ext.make_evaluator()
	
	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
	jec_stack = JECStack(jec_inputs)
	
	name_map = jec_stack.blank_name_map
	name_map['JetPt'] = 'pt'
	name_map['JetMass'] = 'mass'
	name_map['JetEta'] = 'eta'
	name_map['JetA'] = 'area'
	
	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
	#jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
	#name_map['ptGenJet'] = 'pt_gen'
	name_map['ptRaw'] = 'pt_raw'
	name_map['massRaw'] = 'mass_raw'
	name_map['Rho'] = 'rho'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
	        Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs'],
	        Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs'],
	        Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs'],
	        Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs']
	)
	
	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
	
	return corrected_jets

def correctedMCPuppiJets(events,jets):
	ext = extractor()
	ext.add_weight_sets([
	        "* * data/JEC/Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L2Relative_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi.txt"
	])
	
	ext.finalize()
	
	jec_stack_names = [
	        "Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi",
	        "Winter22Run3_V2_MC_L2Relative_AK4PFPuppi",
	        "Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi",
	        "Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi"
	]
	
	evaluator = ext.make_evaluator()
	
	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
	jec_stack = JECStack(jec_inputs)
	
	name_map = jec_stack.blank_name_map
	name_map['JetPt'] = 'pt'
	name_map['JetMass'] = 'mass'
	name_map['JetEta'] = 'eta'
	name_map['JetA'] = 'area'
	
	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
	#jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
	#name_map['ptGenJet'] = 'pt_gen'
	name_map['ptRaw'] = 'pt_raw'
	name_map['massRaw'] = 'mass_raw'
	name_map['Rho'] = 'rho'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
	        Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi'],
	        Winter22Run3_V2_MC_L2Relative_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L2Relative_AK4PFPuppi'],
	        Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi'],
	        Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi']
	)
	
	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
	
	return corrected_jets

def correctedMCChsJets(events,jets):
	ext = extractor()
	ext.add_weight_sets([
	        "* * data/JEC/Winter22Run3_V2_MC_L1FastJet_AK4PFchs.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L2Relative_AK4PFchs.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L3Absolute_AK4PFchs.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L2L3Residual_AK4PFchs.txt"
	])
	
	ext.finalize()
	
	jec_stack_names = [
	        "Winter22Run3_V2_MC_L1FastJet_AK4PFchs",
	        "Winter22Run3_V2_MC_L2Relative_AK4PFchs",
	        "Winter22Run3_V2_MC_L3Absolute_AK4PFchs",
	        "Winter22Run3_V2_MC_L2L3Residual_AK4PFchs"
	]
	
	evaluator = ext.make_evaluator()
	
	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
	jec_stack = JECStack(jec_inputs)
	
	name_map = jec_stack.blank_name_map
	name_map['JetPt'] = 'pt'
	name_map['JetMass'] = 'mass'
	name_map['JetEta'] = 'eta'
	name_map['JetA'] = 'area'
	
	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
	#jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
	#name_map['ptGenJet'] = 'pt_gen'
	name_map['ptRaw'] = 'pt_raw'
	name_map['massRaw'] = 'mass_raw'
	name_map['Rho'] = 'rho'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
	        Winter22Run3_V2_MC_L1FastJet_AK4PFchs=evaluator['Winter22Run3_V2_MC_L1FastJet_AK4PFchs'],
	        Winter22Run3_V2_MC_L2Relative_AK4PFchs=evaluator['Winter22Run3_V2_MC_L2Relative_AK4PFchs'],
	        Winter22Run3_V2_MC_L3Absolute_AK4PFchs=evaluator['Winter22Run3_V2_MC_L3Absolute_AK4PFchs'],
	        Winter22Run3_V2_MC_L2L3Residual_AK4PFchs=evaluator['Winter22Run3_V2_MC_L2L3Residual_AK4PFchs']
	)
	
	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
	
	return corrected_jets
