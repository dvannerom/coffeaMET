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

#from processor import prescales

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

def HLT_SingleMuon(events):
    return (events.HLT.IsoMu24)

def prescale(events):
	ps = np.ones(len(events.HLT.IsoMu24))
	return ps

def dyJetsSelection(events):
	nEvents = len(events)

	# Apply golden json
	lumiMask = LumiMask("data/2022_golden.json")
	#lumiMask = LumiMask("/eos/home-c/cmsdqm/www/CAF/certification/Collisions22/Cert_Collisions2022_eraE_359022_360331_Golden.json")
	events = events[lumiMask(events.run,events.luminosityBlock)]

	nEvents_lumi = len(events)

	# Apply trigger and MET filters
	events = events[METCleaning(events)]
	nEvents_filters = len(events)
	events = events[HLT_SingleMuon(events)]
	nEvents_trigger = len(events)

	# Ignore events with npv == 0
	events = events[(events.PV.npvs > 0)]
	nEvents_npv = len(events)

	# Define good muons
	selectedMuons = events.Muon[(events.Muon.pt >= 10) & (np.abs(events.Muon.eta) < 2.4) & (np.abs(events.Muon.dxy) < 0.045) & (np.abs(events.Muon.dz) < 0.2) & events.Muon.mediumId & (events.Muon.isGlobal | events.Muon.isTracker) & (events.Muon.miniPFRelIso_all < 0.15)]
	# Select events with at least two of them
	events = events[(ak.num(selectedMuons) >= 2)]
	nEvents_2muons = len(events)
	selectedMuons = events.Muon[(events.Muon.pt >= 10) & (np.abs(events.Muon.eta) < 2.4) & (np.abs(events.Muon.dxy) < 0.045) & (np.abs(events.Muon.dz) < 0.2) & events.Muon.mediumId & (events.Muon.isGlobal | events.Muon.isTracker) & (events.Muon.miniPFRelIso_all < 0.15)]
	# The leading muon should have a pT larger than the trigger threshold
	events = events[(selectedMuons.pt[:,0] >= 26)]
	nEvents_leadingmuonPt26 = len(events)
	selectedMuons = events.Muon[(events.Muon.pt >= 10) & (np.abs(events.Muon.eta) < 2.4) & (np.abs(events.Muon.dxy) < 0.045) & (np.abs(events.Muon.dz) < 0.2) & events.Muon.mediumId & (events.Muon.isGlobal | events.Muon.isTracker) & (events.Muon.miniPFRelIso_all < 0.15)]
	# Define lorentz vectors
	leadingMuon = ak.zip(
		{
		    "pt": selectedMuons[:,0].pt,
		    "eta": selectedMuons[:,0].eta,
		    "phi": selectedMuons[:,0].phi,
		    "mass": selectedMuons[:,0].mass,
		},
		with_name="PtEtaPhiMLorentzVector",
	)
	subleadingMuon = ak.zip(
		{
		    "pt": selectedMuons[:,1].pt,
		    "eta": selectedMuons[:,1].eta,
		    "phi": selectedMuons[:,1].phi,
		    "mass": selectedMuons[:,1].mass,
		},
		with_name="PtEtaPhiMLorentzVector",
	)
	Zboson = leadingMuon + subleadingMuon
	deltaR_muons = leadingMuon.delta_r(subleadingMuon)
	invMass = Zboson.mass
	events = events[(deltaR_muons > 0.3) & (invMass > 80) & (invMass < 100)]
	nEvents_muon = len(events)
	
	# Redefine useful collections
	selectedMuons = events.Muon[(events.Muon.pt >= 10) & (np.abs(events.Muon.eta) < 2.4) & (np.abs(events.Muon.dxy) < 0.045) & (np.abs(events.Muon.dz) < 0.2) & events.Muon.mediumId & (events.Muon.isGlobal | events.Muon.isTracker) & (events.Muon.miniPFRelIso_all < 0.15)]
	leadingMuon = ak.zip(
		{
		    "pt": selectedMuons[:,0].pt,
		    "eta": selectedMuons[:,0].eta,
		    "phi": selectedMuons[:,0].phi,
		    "mass": selectedMuons[:,0].mass,
		},
		with_name="PtEtaPhiMLorentzVector",
	)
	subleadingMuon = ak.zip(
		{
		    "pt": selectedMuons[:,1].pt,
		    "eta": selectedMuons[:,1].eta,
		    "phi": selectedMuons[:,1].phi,
		    "mass": selectedMuons[:,1].mass,
		},
		with_name="PtEtaPhiMLorentzVector",
	)
	Zboson = leadingMuon + subleadingMuon
	pv = events.PV
	met_pf_raw = events.RawMET
	met_pf = events.MET
	met_puppi_raw = events.RawPuppiMET
	met_puppi = events.PuppiMET
	
	# Retrieve prescale
	#weights = prescale(events)
	weights = np.ones(len(events))
	
	dyDict = {'events': events,
              'nEvents_tot': nEvents,
              'nEvents_lumi': nEvents_lumi, 'nEvents_filters': nEvents_filters, 'nEvents_trigger': nEvents_trigger, 'nEvents_npv': nEvents_npv, 'nEvents_muon': nEvents_muon,
              'weights': weights, 'boson': Zboson, 'pv': pv, 'met_pf_raw': met_pf_raw, 'met_pf': met_pf, 'met_puppi_raw': met_puppi_raw, 'met_puppi': met_puppi}
	
	return dyDict
