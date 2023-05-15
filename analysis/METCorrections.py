import os, sys

import awkward as ak
from coffea.jetmet_tools import FactorizedJetCorrector, JetCorrectionUncertainty
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory
from coffea.jetmet_tools import CorrectedMETFactory
from coffea.lookup_tools import extractor

import array
import json
import numpy as np
import cachetools
from cachetools import LRUCache

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
	#mask = (pt>15) & ((nhf<0.8) & (chf>0.1) & (nef<0.9) & (cef<0.9))
	mask = (pt>15) & (eta > -5.2) & (eta < 5.2) & (cef < 0.9) & (muf < 0.9)
	return mask

def recoil(met, boson):
	vec_recoil = ak.zip(
	        {
	                "x": -met.px - boson.x,
	                "y": -met.py - boson.y,
	        },
	        with_name="TwoVector",
	)
	return vec_recoil

def met_TypeI(met_raw,jets,corrected_jets,var):
	raw_jets_px = (1-jets.rawFactor)*jets.pt*np.cos(jets.phi)
	raw_jets_py = (1-jets.rawFactor)*jets.pt*np.sin(jets.phi)
	#raw_jets_px = jets.pt*np.cos(jets.phi)
	#raw_jets_py = jets.pt*np.sin(jets.phi)
	raw_jets_px = ak.sum(raw_jets_px,-1)
	raw_jets_py = ak.sum(raw_jets_py,-1)
	corrected_jets_px = corrected_jets.pt*np.cos(corrected_jets.phi)
	corrected_jets_py = corrected_jets.pt*np.sin(corrected_jets.phi)
	corrected_jets_px = ak.sum(corrected_jets_px,-1)
	corrected_jets_py = ak.sum(corrected_jets_py,-1)

	if var == 1:
		corrected_jets_px = ak.sum(corrected_jets.JES_jes.up.px,-1)
		corrected_jets_py = ak.sum(corrected_jets.JES_jes.up.py,-1)
	elif var == -1:
		corrected_jets_px = ak.sum(corrected_jets.JES_jes.down.px,-1)
		corrected_jets_py = ak.sum(corrected_jets.JES_jes.down.py,-1)

	vec_met_TypeI = ak.zip(
		{
		        "x": met_raw.pt*np.cos(met_raw.phi) + raw_jets_px - corrected_jets_px,
		        "y": met_raw.pt*np.sin(met_raw.phi) + raw_jets_py - corrected_jets_py
		},
		with_name="TwoVector",
	)

	return vec_met_TypeI

def correctedDataPuppiMET(events,met,jets):
	ext = extractor()
	ext.add_weight_sets([
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_Uncertainty_AK4PFPuppi.junc.txt",
	])
	ext.finalize()
	
	jec_stack_names = [
	        "Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi",
	        "Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi",
	        "Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi",
	        "Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi",
	        "Winter22Run3_RunC_V2_DATA_Uncertainty_AK4PFPuppi"
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

	name_map['METpt'] = 'pt'
	name_map['JetPhi'] = 'phi'
	name_map['METphi'] = 'phi'
	name_map['UnClusteredEnergyDeltaX'] = 'UnClusteredEnergyDeltaX'
	name_map['UnClusteredEnergyDeltaY'] = 'UnClusteredEnergyDeltaY'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
	        Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFPuppi'],
	        Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFPuppi'],
	        Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFPuppi'],
	        Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFPuppi']
	)
	uncertainties = JetCorrectionUncertainty(
		Winter22Run3_RunC_V2_DATA_Uncertainty_AK4PFPuppi=evaluator['Winter22Run3_RunC_V2_DATA_Uncertainty_AK4PFPuppi']
	)

	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
	return corrected_jets

	#met_factory = CorrectedMETFactory(name_map)
	#corrected_met = met_factory.build(met, corrected_jets, lazy_cache=events_cache)
	#
	#return corrected_met


def correctedMCPuppiMET(events,met,jets):
	ext = extractor()
	ext.add_weight_sets([
	        "* * data/JEC/Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L2Relative_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi.txt",
	        "* * data/JEC/Winter22Run3_V2_MC_Uncertainty_AK4PFPuppi.junc.txt",
	])
	
	ext.finalize()
	
	jec_stack_names = [
	        "Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi",
	        "Winter22Run3_V2_MC_L2Relative_AK4PFPuppi",
	        "Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi",
	        "Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi",
	        "Winter22Run3_V2_MC_Uncertainty_AK4PFPuppi"
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
	
	name_map['METpt'] = 'pt'
	name_map['JetPhi'] = 'phi'
	name_map['METphi'] = 'phi'
	name_map['UnClusteredEnergyDeltaX'] = 'UnClusteredEnergyDeltaX'
	name_map['UnClusteredEnergyDeltaY'] = 'UnClusteredEnergyDeltaY'
	
	events_cache = events.caches[0]
	corrector = FactorizedJetCorrector(
	        Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L1FastJet_AK4PFPuppi'],
	        Winter22Run3_V2_MC_L2Relative_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L2Relative_AK4PFPuppi'],
	        Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L3Absolute_AK4PFPuppi'],
	        Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_L2L3Residual_AK4PFPuppi']
	)
	uncertainties = JetCorrectionUncertainty(
		Winter22Run3_V2_MC_Uncertainty_AK4PFPuppi=evaluator['Winter22Run3_V2_MC_Uncertainty_AK4PFPuppi']
	)

	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
	return corrected_jets

	#met_factory = CorrectedMETFactory(name_map)
	#corrected_met = met_factory.build(met, jets, {})
	#
	#return corrected_met

#def correctedDataPFMET(events,met,jets):
#	ext = extractor()
#	ext.add_weight_sets([
#	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs.txt",
#	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs.txt",
#	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs.txt",
#	        "* * data/JEC/Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs.txt"
#	])
#	
#	ext.finalize()
#	
#	jec_stack_names = [
#	        "Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs",
#	        "Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs",
#	        "Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs",
#	        "Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs"
#	]
#	
#	evaluator = ext.make_evaluator()
#	
#	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
#	jec_stack = JECStack(jec_inputs)
#	
#	name_map = jec_stack.blank_name_map
#	name_map['JetPt'] = 'pt'
#	name_map['JetMass'] = 'mass'
#	name_map['JetEta'] = 'eta'
#	name_map['JetA'] = 'area'
#	
#	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
#	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
#	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
#	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
#	#jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
#	#name_map['ptGenJet'] = 'pt_gen'
#	name_map['ptRaw'] = 'pt_raw'
#	name_map['massRaw'] = 'mass_raw'
#	name_map['Rho'] = 'rho'
#	
#	events_cache = events.caches[0]
#	corrector = FactorizedJetCorrector(
#	        Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L1FastJet_AK4PFchs'],
#	        Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L2Relative_AK4PFchs'],
#	        Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L3Absolute_AK4PFchs'],
#	        Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs=evaluator['Winter22Run3_RunC_V2_DATA_L2L3Residual_AK4PFchs']
#	)
#	
#	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
#	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
#	
#	return corrected_jets
#
#def correctedMCPFMET(events,met,jets):
##def correctedMCPFMET(events,jets):
#	ext = extractor()
#	ext.add_weight_sets([
#	        "* * data/JEC/Winter22Run3_V2_MC_L1FastJet_AK4PFchs.txt",
#	        "* * data/JEC/Winter22Run3_V2_MC_L2Relative_AK4PFchs.txt",
#	        "* * data/JEC/Winter22Run3_V2_MC_L3Absolute_AK4PFchs.txt",
#	        "* * data/JEC/Winter22Run3_V2_MC_L2L3Residual_AK4PFchs.txt"
#	])
#	
#	ext.finalize()
#	
#	jec_stack_names = [
#	        "Winter22Run3_V2_MC_L1FastJet_AK4PFchs",
#	        "Winter22Run3_V2_MC_L2Relative_AK4PFchs",
#	        "Winter22Run3_V2_MC_L3Absolute_AK4PFchs",
#	        "Winter22Run3_V2_MC_L2L3Residual_AK4PFchs"
#	]
#	
#	evaluator = ext.make_evaluator()
#	
#	jec_inputs = {name: evaluator[name] for name in jec_stack_names}
#	jec_stack = JECStack(jec_inputs)
#	
#	name_map = jec_stack.blank_name_map
#	name_map['JetPt'] = 'pt'
#	name_map['JetMass'] = 'mass'
#	name_map['JetEta'] = 'eta'
#	name_map['JetA'] = 'area'
#	
#	jets['pt_raw'] = (1 - jets['rawFactor']) * jets['pt']
#	jets['mass_raw'] = (1 - jets['rawFactor']) * jets['mass']
#	#jets['pt_gen'] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
#	jets['rho'] = ak.broadcast_arrays(events.Rho.fixedGridRhoFastjetAll, jets.pt)[0]
#	#jets['rho'] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
#	#name_map['ptGenJet'] = 'pt_gen'
#	name_map['ptRaw'] = 'pt_raw'
#	name_map['massRaw'] = 'mass_raw'
#	name_map['Rho'] = 'rho'
#	
#	events_cache = events.caches[0]
#	corrector = FactorizedJetCorrector(
#	        Winter22Run3_V2_MC_L1FastJet_AK4PFchs=evaluator['Winter22Run3_V2_MC_L1FastJet_AK4PFchs'],
#	        Winter22Run3_V2_MC_L2Relative_AK4PFchs=evaluator['Winter22Run3_V2_MC_L2Relative_AK4PFchs'],
#	        Winter22Run3_V2_MC_L3Absolute_AK4PFchs=evaluator['Winter22Run3_V2_MC_L3Absolute_AK4PFchs'],
#	        Winter22Run3_V2_MC_L2L3Residual_AK4PFchs=evaluator['Winter22Run3_V2_MC_L2L3Residual_AK4PFchs']
#	)
#	
#	jet_factory = CorrectedJetsFactory(name_map, jec_stack)
#	corrected_jets = jet_factory.build(jets, lazy_cache=events_cache)
#	
#	return corrected_jets
