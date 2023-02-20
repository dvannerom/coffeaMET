import os, sys

import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea import processor, hist, util
from coffea.nanoevents.methods import vector
from coffea.nanoevents.methods.vector import ThreeVector
from coffea.lumi_tools import LumiMask

import array
import json
import numpy as np

from processor import prescales

def METCleaning(events):
    return (events.Flag.goodVertices &
            events.Flag.globalSuperTightHalo2016Filter &
            events.Flag.HBHENoiseFilter &
            events.Flag.HBHENoiseIsoFilter &
            events.Flag.BadPFMuonFilter &
            events.Flag.ecalBadCalibFilter &
            events.Flag.EcalDeadCellTriggerPrimitiveFilter &
            events.Flag.BadPFMuonDzFilter &
            events.Flag.eeBadScFilter
    )

def HLT_SinglePhoton(events):
    return (events.HLT.Photon50_R9Id90_HE10_IsoM |
            events.HLT.Photon75_R9Id90_HE10_IsoM |
            events.HLT.Photon90_R9Id90_HE10_IsoM |
            events.HLT.Photon120_R9Id90_HE10_IsoM |
            events.HLT.Photon165_R9Id90_HE10_IsoM 
    )

def prescale(events):
	ps = np.ones(len(events.HLT.Photon165_R9Id90_HE10_IsoM))
	# 2016
#	for i in range(len(events.HLT.Photon165_R9Id90_HE10_IsoM)):
#		if events.HLT.Photon165_R9Id90_HE10_IsoM[i]: ps[i] = 1
#		else:
#			if events.HLT.Photon120_R9Id90_HE10_IsoM[i]: ps[i] = 2#3#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon120_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
#			else:
#				if events.HLT.Photon90_R9Id90_HE10_IsoM[i]: ps[i] = 6#8.8#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon90_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
#				else:
#					if events.HLT.Photon75_R9Id90_HE10_IsoM[i]: ps[i] = 12#17.6#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon75_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
#					else:
#						if events.HLT.Photon50_R9Id90_HE10_IsoM[i]: ps[i] = 60#80#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon50_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
#						else: ps[i] = 0
	# 2018
	for i in range(len(events.HLT.Photon165_R9Id90_HE10_IsoM)):
		if events.HLT.Photon165_R9Id90_HE10_IsoM[i]: ps[i] = 4
		else:
			if events.HLT.Photon120_R9Id90_HE10_IsoM[i]: ps[i] = 8#3#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon120_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
			else:
				if events.HLT.Photon90_R9Id90_HE10_IsoM[i]: ps[i] = 32#8.8#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon90_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
				else:
					if events.HLT.Photon75_R9Id90_HE10_IsoM[i]: ps[i] = 64#17.6#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon75_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
					else:
						if events.HLT.Photon50_R9Id90_HE10_IsoM[i]: ps[i] = 256#80#prescales.getPrescale(str(events[i].run),events[i].luminosityBlock,"HLT_Photon50_R9Id90_HE10_IsoM_v","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_runInfo.json","/user/vannerom/MET/decafMET/analysis/data/triggerData2016_hltprescales.json")
						else: ps[i] = 0
	return ps

def gammaJetsSelection(events):
	nEvents = len(events)

	# Apply golden json
	#lumiMask = LumiMask("data/2016_golden.json")
	lumiMask = LumiMask("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt")
	events = events[lumiMask(events.run,events.luminosityBlock)]

	nEvents_lumi = len(events)

	# Apply trigger and MET filters
	events = events[METCleaning(events)]
	nEvents_filters = len(events)
	events = events[HLT_SinglePhoton(events)]
	nEvents_trigger = len(events)

	# Ignore events with npv == 0
	events = events[(events.PV.npvs > 0)]
	nEvents_npv = len(events)
	
	# Define photon selection
	selectedPhotons = events.Photon
	selectedPhotons = selectedPhotons[(selectedPhotons.pt >= 50) & (np.abs(selectedPhotons.eta) < 1.44) & (selectedPhotons.cutBased == 3)]
	events = events[(ak.num(selectedPhotons) == 1)]
	nEvents_photon = len(events)
	
	# Define lepton veto
	selectedElectrons = events.Electron
	selectedElectrons = selectedElectrons[(selectedElectrons.pt >= 10) & (selectedElectrons.cutBased == 2)]
	selectedMuons = events.Muon
	selectedMuons = selectedMuons[(selectedMuons.pt >= 10) & selectedMuons.looseId]
	events = events[(ak.num(selectedElectrons) == 0) & (ak.num(selectedMuons) == 0)]
	nEvents_lepton = len(events)
	
	# Define jet selection
	selectedJets = events.Jet
	selectedJets = selectedJets[(selectedJets.pt >= 40) & (np.abs(selectedJets.eta) < 2.5) & (selectedJets.jetId == 6)]
	events = events[(ak.num(selectedJets) >= 1)]
	nEvents_jet = len(events)
	
	# Compute deltaR between photon and leading jet to mitigate QCD
	photon = events.Photon[(events.Photon.pt >= 50) & (np.abs(events.Photon.eta) < 1.44) & (events.Photon.cutBased == 3)]
	leadingJet = events.Jet[:,0]
	photonLorentz = ak.zip(
		{
			"pt": photon.pt,
			"eta": photon.eta,
			"phi": photon.phi,
			"mass": photon.mass,
		},
		with_name="PtEtaPhiMLorentzVector",
	)
	leadingJetLorentz = ak.zip(
		{
			"pt": leadingJet.pt,
			"eta": leadingJet.eta,
			"phi": leadingJet.phi,
			"mass": leadingJet.mass,
		},
		with_name="PtEtaPhiMLorentzVector",
	)
	deltaR_photon_jet = photonLorentz.delta_r(leadingJetLorentz)
	
	# Apply event selection and redefine useful collections
	events = events[deltaR_photon_jet[:,0] > 0.5]
	nEvents_deltaR = len(events)
	photon = events.Photon[(events.Photon.pt >= 50) & (np.abs(events.Photon.eta) < 1.44) & (events.Photon.cutBased == 3)]
	pv = events.PV
	met_pf_raw = events.RawMET
	met_pf = events.MET
	met_puppi_raw = events.RawPuppiMET
	met_puppi = events.PuppiMET
	
	# Retrieve prescale
	weights = prescale(events)
	#weights = np.ones(len(events))
	
	gammaDict = {'events': events,
                     'nEvents_tot': nEvents,
                     'nEvents_lumi': nEvents_lumi, 'nEvents_filters': nEvents_filters, 'nEvents_trigger': nEvents_trigger, 'nEvents_npv': nEvents_npv, 'nEvents_photon': nEvents_photon, 'nEvents_lepton': nEvents_lepton, 'nEvents_jet': nEvents_jet, 'nEvents_deltaR': nEvents_deltaR,
                      'weights': weights, 'boson': photon, 'pv': pv, 'met_pf_raw': met_pf_raw, 'met_pf': met_pf, 'met_puppi_raw': met_puppi_raw, 'met_puppi': met_puppi}
	
	return gammaDict
