import os, sys

import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea import processor, hist, util
from coffea.nanoevents.methods import vector
from coffea.nanoevents.methods.vector import ThreeVector
from coffea.jetmet_tools import FactorizedJetCorrector, JetCorrectionUncertainty
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory
from coffea.lookup_tools import extractor
from coffea.analysis_tools import PackedSelection

import utilsJEC, METCorrections

import uproot3
import hist2root
import ROOT
from ROOT import TFile

import array
import json
import numpy as np

from processors import gammaJets, dyJets

class performanceProcessor(processor.ProcessorABC):
	def __init__(self,isData):
		self.isData = isData
		self._histo1 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("met_pf_raw_phi", "Phi (Raw PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_pf_phi", "Phi (PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_raw_phi", "Phi (Raw Puppi missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_phi", "Phi (Puppi missing ET)", 40, -3.2, 3.2),
		)
		self._histo2 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("met_pf_phi_TypeI", "Phi (PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_phi_TypeI", "Phi (Puppi missing ET)", 40, -3.2, 3.2),
		)
		self._histo3 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("met_pf_phi_TypeI_JESUp", "Phi (PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_phi_TypeI_JESUp", "Phi (Puppi missing ET)", 40, -3.2, 3.2),
		)
		self._histo4 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("met_pf_phi_TypeI_JESDown", "Phi (PF missing ET)", 40, -3.2, 3.2),
			hist.Bin("met_puppi_phi_TypeI_JESDown", "Phi (Puppi missing ET)", 40, -3.2, 3.2),
		)
		self._histo5 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf_raw", "Raw missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf_raw", "Response (RAW)", 500, -50, 50),
		)
		self._histo6 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi_raw", "Raw missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_puppi_raw", "Response (RAW)", 500, -50, 50),
		)
		self._histo7 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf", "Response (PF)", 500, -50, 50),
		)
		self._histo8 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf_TypeI", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf_TypeI", "Response (PF)", 500, -50, 50),
		)
		self._histo9 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf_TypeI_JESUp", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf_TypeI_JESUp", "Response (PF)", 500, -50, 50),
		)
		self._histo10 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_pf_TypeI_JESDown", "PF missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_pf_TypeI_JESDown", "Response (PF)", 500, -50, 50),
		)
		self._histo11 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_puppi", "Response (PUPPI)", 500, -50, 50),
		)
		self._histo12 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi_TypeI", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_puppi_TypeI", "Response (PUPPI)", 500, -50, 50),
		)
		self._histo13 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi_TypeI_JESUp", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_puppi_TypeI_JESUp", "Response (PUPPI)", 500, -50, 50),
		)
		self._histo14 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("met_puppi_TypeI_JESDown", "Puppi missing ET", np.array([0,10,20,30,40,50,75,100,125,150,200,300])),
			hist.Bin("response_puppi_TypeI_JESDown", "Response (PUPPI)", 500, -50, 50),
		)
		self._histo15 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_pf_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
			hist.Bin("upar_puppi_raw_plus_qt", "Parallel recoil (RAW)", 100, -200, 200),
		)
		self._histo16 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_pf_plus_qt", "Parallel recoil (PF)", 100, -200, 200),
			hist.Bin("upar_pf_plus_qt_TypeI", "Parallel recoil (PF)", 100, -200, 200),
		)
		self._histo17 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_pf_plus_qt_TypeI_JESUp", "Parallel recoil (PF)", 100, -200, 200),
			hist.Bin("upar_pf_plus_qt_TypeI_JESDown", "Parallel recoil (PF)", 100, -200, 200),
		)
		self._histo18 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_puppi_plus_qt", "Parallel recoil (PUPPI)", 100, -200, 200),
			hist.Bin("upar_puppi_plus_qt_TypeI", "Parallel recoil (PUPPI)", 100, -200, 200),
		)
		self._histo19 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("upar_puppi_plus_qt_TypeI_JESUp", "Parallel recoil (PUPPI)", 100, -200, 200),
			hist.Bin("upar_puppi_plus_qt_TypeI_JESDown", "Parallel recoil (PUPPI)", 100, -200, 200),
		)
		self._histo20 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_pf_raw", "Perpendicular recoil (RAW)", 50, -200, 200),
			hist.Bin("uperp_puppi_raw", "Perpendicular recoil (RAW)", 50, -200, 200),
		)
		self._histo21 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_pf", "Perpendicular recoil (PF)", 50, -200, 200),
			hist.Bin("uperp_pf_TypeI", "Perpendicular recoil (PF)", 50, -200, 200),
		)
		self._histo22 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_pf_TypeI_JESUp", "Perpendicular recoil (PF)", 50, -200, 200),
			hist.Bin("uperp_pf_TypeI_JESDown", "Perpendicular recoil (PF)", 50, -200, 200),
		)
		self._histo23 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_puppi", "Perpendicular recoil (PUPPI)", 50, -200, 200),
			hist.Bin("uperp_puppi_TypeI", "Perpendicular recoil (PUPPI)", 50, -200, 200),
		)
		self._histo24 = hist.Hist(
			"Events",
			hist.Cat("dataset", "Dataset"),
			hist.Bin("qt", "Photon pT", np.array([0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,80,90,100,125,150,200,300,400,500])),
			hist.Bin("pv", "Number of reconstructed PV", 20, 0, 80),
			hist.Bin("uperp_puppi_TypeI_JESUp", "Perpendicular recoil (PUPPI)", 50, -200, 200),
			hist.Bin("uperp_puppi_TypeI_JESDown", "Perpendicular recoil (PUPPI)", 50, -200, 200),
		)
		self._histo25 = hist.Hist(
			"Events",
			hist.Bin("nEventsTot", "Total number of events", 1, 0, 1),
		)
		self._histo26 = hist.Hist(
			"Events",
			hist.Bin("nEventsLumi", "Number of events after json", 1, 0, 1),
		)
		self._histo27 = hist.Hist(
			"Events",
			hist.Bin("nEventsFilters", "Number of events after MET filters", 1, 0, 1),
		)
		self._histo28 = hist.Hist(
			"Events",
			hist.Bin("nEventsTriggers", "Number of events after triggers", 1, 0, 1),
		)
		self._histo29 = hist.Hist(
			"Events",
			hist.Bin("nEventsNPV", "Number of events after npv cut", 1, 0, 1),
		)
		# DY
		self._histo30 = hist.Hist(
			"Events",
			hist.Bin("nEventsMuon", "Number of events after muon selection", 1, 0, 1),
		)
		# Gamma
		#self._histo20 = hist.Hist(
		#	"Events",
		#	hist.Bin("nEventsPhoton", "Number of events after photon selection", 1, 0, 1),
		#)
		#self._histo21 = hist.Hist(
		#	"Events",
		#	hist.Bin("nEventsLepton", "Number of events after lepton veto", 1, 0, 1),
		#)
		#self._histo22 = hist.Hist(
		#	"Events",
		#	hist.Bin("nEventsJet", "Number of events after jet veto", 1, 0, 1),
		#)
		#self._histo23 = hist.Hist(
		#	"Events",
		#	hist.Bin("nEventsDeltaR", "Number of events after deltaR selection", 1, 0, 1),
		#)
		
		#self._accumulator = processor.dict_accumulator({
		#	'histo1':self._histo1,
		#	'histo2':self._histo2,
		#	'histo3':self._histo3,
		#	'histo4':self._histo4,
		#	'histo5':self._histo5,
		#	'histo6':self._histo6,
		#	'histo7':self._histo7,
		#	'histo8':self._histo8,
		#	'histo9':self._histo9,
		#	'histo10':self._histo10,
		#	'histo11':self._histo11,
		#	'histo12':self._histo12,
		#	'histo13':self._histo13,
		#	'histo14':self._histo14,
		#	'histo15':self._histo15,
		#	'histo16':self._histo16,
		#	'histo17':self._histo17,
		#	'histo18':self._histo18,
		#	'histo19':self._histo19,
		#	'histo20':self._histo20,
		#	'histo21':self._histo21,
		#	'histo22':self._histo22,
		#	'histo23':self._histo23,
		#	'histo24':self._histo24,
		#	'histo25':self._histo25,
		#	'histo26':self._histo26,
		#	'histo27':self._histo27,
		#	'histo28':self._histo28,
		#	'histo29':self._histo29,
		#	'histo30':self._histo30,
		#	#'histo21':self._histo21,
		#	#'histo22':self._histo22,
		#	#'histo23':self._histo23,
		#})

	@property
	def accumulator(self):
		#return self._histo
		#return self._accumulator
		return {
			'histo1':self._histo1,
			'histo2':self._histo2,
			'histo3':self._histo3,
			'histo4':self._histo4,
			'histo5':self._histo5,
			'histo6':self._histo6,
			'histo7':self._histo7,
			'histo8':self._histo8,
			'histo9':self._histo9,
			'histo10':self._histo10,
			'histo11':self._histo11,
			'histo12':self._histo12,
			'histo13':self._histo13,
			'histo14':self._histo14,
			'histo15':self._histo15,
			'histo16':self._histo16,
			'histo17':self._histo17,
			'histo18':self._histo18,
			'histo19':self._histo19,
			'histo20':self._histo20,
			'histo21':self._histo21,
			'histo22':self._histo22,
			'histo23':self._histo23,
			'histo24':self._histo24,
			'histo25':self._histo25,
			'histo26':self._histo26,
			'histo27':self._histo27,
			'histo28':self._histo28,
			'histo29':self._histo29,
			'histo30':self._histo30
		}

	# we will receive a NanoEvents instead of a coffea DataFrame
	def process(self, events):
		out = self.accumulator#.identity()

		# For DY
		nEventsTot = dyJets.dyJetsSelection(events,self.isData)['nEvents_tot']
		nEventsLumi = dyJets.dyJetsSelection(events,self.isData)['nEvents_lumi']
		nEventsFilters = dyJets.dyJetsSelection(events,self.isData)['nEvents_filters']
		nEventsTriggers = dyJets.dyJetsSelection(events,self.isData)['nEvents_trigger']
		nEventsNPV = dyJets.dyJetsSelection(events,self.isData)['nEvents_npv']
		nEventsMuon = dyJets.dyJetsSelection(events,self.isData)['nEvents_muon']
		nEventsTot = np.zeros(nEventsTot)
		nEventsLumi = np.zeros(nEventsLumi)
		nEventsFilters = np.zeros(nEventsFilters)
		nEventsTriggers = np.zeros(nEventsTriggers)
		nEventsNPV = np.zeros(nEventsNPV)
		nEventsMuon = np.zeros(nEventsMuon)

		# For Gamma
		#nEventsTot = gammaJets.gammaJetsSelection(events)['nEvents_tot']
		#nEventsLumi = gammaJets.gammaJetsSelection(events)['nEvents_lumi']
		#nEventsFilters = gammaJets.gammaJetsSelection(events)['nEvents_filters']
		#nEventsTriggers = gammaJets.gammaJetsSelection(events)['nEvents_trigger']
		#nEventsNPV = gammaJets.gammaJetsSelection(events)['nEvents_npv']
		#nEventsPhoton = gammaJets.gammaJetsSelection(events)['nEvents_photon']
		#nEventsLepton = gammaJets.gammaJetsSelection(events)['nEvents_lepton']
		#nEventsJet = gammaJets.gammaJetsSelection(events)['nEvents_jet']
		#nEventsDeltaR = gammaJets.gammaJetsSelection(events)['nEvents_deltaR']
		#nEventsTot = np.zeros(nEventsTot)
		#nEventsLumi = np.zeros(nEventsLumi)
		#nEventsFilters = np.zeros(nEventsFilters)
		#nEventsTriggers = np.zeros(nEventsTriggers)
		#nEventsNPV = np.zeros(nEventsNPV)
		#nEventsPhoton = np.zeros(nEventsPhoton)
		#nEventsLepton = np.zeros(nEventsLepton)
		#nEventsJet = np.zeros(nEventsJet)
		#nEventsDeltaR = np.zeros(nEventsDeltaR)

		# For DY
		events = dyJets.dyJetsSelection(events,self.isData)['events']
		if len(events) == 0:
			return out
		boson = dyJets.dyJetsSelection(events,self.isData)['boson']
		boson_pt = boson.pt
		pv = dyJets.dyJetsSelection(events,self.isData)['pv']
		met_pf_raw = events.RawMET
		met_pf = events.MET
		met_puppi_raw = events.RawPuppiMET
		met_puppi = events.PuppiMET

		# For Gamma 
		#events = gammaJets.gammaJetsSelection(events)['events']
		#boson = gammaJets.gammaJetsSelection(events)['boson']
		#boson_pt = boson.pt[:,0]
		#pv = gammaJets.gammaJetsSelection(events)['pv']
		#met_pf_raw = gammaJets.gammaJetsSelection(events)['met_pf_raw']
		#met_pf = gammaJets.gammaJetsSelection(events)['met_pf']
		#met_puppi_raw = gammaJets.gammaJetsSelection(events)['met_puppi_raw']
		#met_puppi = gammaJets.gammaJetsSelection(events)['met_puppi']

		# JMENano v9
		#chsJets = events.Jet[utilsJEC.isGoodJet(events.Jet)]
		#puppiJets = events.JetPuppi[utilsJEC.isGoodJet(events.JetPuppi)]
		# JMENano >= v10
		chsJets = events.JetCHS[utilsJEC.isGoodJet(events.JetCHS)]
		puppiJets = events.Jet[utilsJEC.isGoodJet(events.Jet)]

		corrected_chsJets = chsJets
		if self.isData: corrected_puppiJets = METCorrections.correctedDataPuppiMET(events,met_puppi_raw,puppiJets)
		else: corrected_puppiJets = METCorrections.correctedMCPuppiMET(events,met_puppi_raw,puppiJets)

		met_pf_TypeI = METCorrections.met_TypeI(met_pf_raw,chsJets,corrected_chsJets,0)
		met_pf_TypeI_JESUp = METCorrections.met_TypeI(met_pf_raw,chsJets,corrected_chsJets,0)#METCorrections.met_TypeI(met_pf_raw,chsJets,corrected_chsJets,1)
		met_pf_TypeI_JESDown = METCorrections.met_TypeI(met_pf_raw,chsJets,corrected_chsJets,0)#METCorrections.met_TypeI(met_pf_raw,chsJets,corrected_chsJets,-1)
		met_puppi_TypeI = METCorrections.met_TypeI(met_puppi_raw,puppiJets,corrected_puppiJets,0)
		met_puppi_TypeI_JESUp = METCorrections.met_TypeI(met_puppi_raw,puppiJets,corrected_puppiJets,1)
		met_puppi_TypeI_JESDown = METCorrections.met_TypeI(met_puppi_raw,puppiJets,corrected_puppiJets,-1)
	
		# Implement MET XY corrections
		#print(type(METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv.npvs,events.run,False,"2016preVFP",True,ispuppi=False)),METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv.npvs,events.run,False,"2016preVFP",True,ispuppi=False))
		#met_pf.px = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,0]
		#met_pf.py = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,1]
		#met_pf.pt = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,2]
		#met_pf.phi = METCorrections.correctedMET(met_pf.pt,met_pf.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)[:,3]
		#met_puppi = METCorrections.correctedMET(met_puppi.pt,met_puppi.phi,pv,events.run,False,"2016preVFP",True,ispuppi=False)

		# Compute hadronic recoil and components
		vec_boson = ak.zip(
			{
				"x": boson.px,
				"y": boson.py,
			},
			with_name="TwoVector",
		)
		vec_boson_unit = vec_boson.unit
	
		# PF quantities
		# Raw
		vec_u_pf_raw = METCorrections.recoil(met_pf_raw,vec_boson)
		upar_pf_raw = (vec_u_pf_raw.x*vec_boson_unit.x) + (vec_u_pf_raw.y*vec_boson_unit.y)
		upar_pf_raw_plus_qt = upar_pf_raw + boson_pt
		upar_pf_raw = -upar_pf_raw
		response_pf_raw = upar_pf_raw/boson_pt
		uperp_pf_raw =  (vec_u_pf_raw.y*vec_boson_unit.x) - (vec_u_pf_raw.x*vec_boson_unit.y)
		# Default corrected
		vec_u_pf = METCorrections.recoil(met_pf,vec_boson)
		upar_pf = (vec_u_pf.x*vec_boson_unit.x) + (vec_u_pf.y*vec_boson_unit.y)
		upar_pf_plus_qt = upar_pf + boson_pt
		upar_pf = -upar_pf
		response_pf = upar_pf/boson_pt
		uperp_pf = (vec_u_pf.y*vec_boson_unit.x) - (vec_u_pf.x*vec_boson_unit.y)
		# Type-I corrected
		vec_u_pf_TypeI = METCorrections.recoil(met_pf_TypeI,vec_boson)
		upar_pf_TypeI = (vec_u_pf_TypeI.x*vec_boson_unit.x) + (vec_u_pf_TypeI.y*vec_boson_unit.y)
		upar_pf_TypeI = (vec_u_pf_TypeI.x*vec_boson_unit.x) + (vec_u_pf_TypeI.y*vec_boson_unit.y)
		upar_pf_plus_qt_TypeI = upar_pf_TypeI + boson_pt
		upar_pf_TypeI = -upar_pf_TypeI
		response_pf_TypeI = upar_pf_TypeI/boson_pt
		uperp_pf_TypeI = (vec_u_pf_TypeI.y*vec_boson_unit.x) - (vec_u_pf_TypeI.x*vec_boson_unit.y)
		# Type-I JESUp corrected
		vec_u_pf_TypeI_JESUp = METCorrections.recoil(met_pf_TypeI_JESUp,vec_boson)
		upar_pf_TypeI_JESUp = (vec_u_pf_TypeI_JESUp.x*vec_boson_unit.x) + (vec_u_pf_TypeI_JESUp.y*vec_boson_unit.y)
		upar_pf_TypeI_JESUp = (vec_u_pf_TypeI_JESUp.x*vec_boson_unit.x) + (vec_u_pf_TypeI_JESUp.y*vec_boson_unit.y)
		upar_pf_plus_qt_TypeI_JESUp = upar_pf_TypeI_JESUp + boson_pt
		upar_pf_TypeI_JESUp = -upar_pf_TypeI_JESUp
		response_pf_TypeI_JESUp = upar_pf_TypeI_JESUp/boson_pt
		uperp_pf_TypeI_JESUp = (vec_u_pf_TypeI_JESUp.y*vec_boson_unit.x) - (vec_u_pf_TypeI_JESUp.x*vec_boson_unit.y)
		# Type-I JESDown corrected
		vec_u_pf_TypeI_JESDown = METCorrections.recoil(met_pf_TypeI_JESDown,vec_boson)
		upar_pf_TypeI_JESDown = (vec_u_pf_TypeI_JESDown.x*vec_boson_unit.x) + (vec_u_pf_TypeI_JESDown.y*vec_boson_unit.y)
		upar_pf_TypeI_JESDown = (vec_u_pf_TypeI_JESDown.x*vec_boson_unit.x) + (vec_u_pf_TypeI_JESDown.y*vec_boson_unit.y)
		upar_pf_plus_qt_TypeI_JESDown = upar_pf_TypeI_JESDown + boson_pt
		upar_pf_TypeI_JESDown = -upar_pf_TypeI_JESDown
		response_pf_TypeI_JESDown = upar_pf_TypeI_JESDown/boson_pt
		uperp_pf_TypeI_JESDown = (vec_u_pf_TypeI_JESDown.y*vec_boson_unit.x) - (vec_u_pf_TypeI_JESDown.x*vec_boson_unit.y)
		# PUPPI quantities
		# Raw
		vec_u_puppi_raw = METCorrections.recoil(met_puppi_raw,vec_boson)
		upar_puppi_raw = (vec_u_puppi_raw.x*vec_boson_unit.x) + (vec_u_puppi_raw.y*vec_boson_unit.y)
		upar_puppi_raw_plus_qt = upar_puppi_raw + boson_pt
		upar_puppi_raw = -upar_puppi_raw
		response_puppi_raw = upar_puppi_raw/boson_pt
		uperp_puppi_raw =  (vec_u_puppi_raw.y*vec_boson_unit.x) - (vec_u_puppi_raw.x*vec_boson_unit.y)
		# Default corrected
		vec_u_puppi = METCorrections.recoil(met_puppi,vec_boson)
		upar_puppi = (vec_u_puppi.x*vec_boson_unit.x) + (vec_u_puppi.y*vec_boson_unit.y)
		upar_puppi_plus_qt = upar_puppi + boson_pt
		upar_puppi = -upar_puppi
		response_puppi = upar_puppi/boson_pt
		uperp_puppi = (vec_u_puppi.y*vec_boson_unit.x) - (vec_u_puppi.x*vec_boson_unit.y)
		# Type-I corrected
		vec_u_puppi_TypeI = METCorrections.recoil(met_puppi_TypeI,vec_boson)
		upar_puppi_TypeI = (vec_u_puppi_TypeI.x*vec_boson_unit.x) + (vec_u_puppi_TypeI.y*vec_boson_unit.y)
		upar_puppi_plus_qt_TypeI = upar_puppi_TypeI + boson_pt
		upar_puppi_TypeI = -upar_puppi_TypeI
		response_puppi_TypeI = upar_puppi_TypeI/boson_pt
		uperp_puppi_TypeI = (vec_u_puppi_TypeI.y*vec_boson_unit.x) - (vec_u_puppi_TypeI.x*vec_boson_unit.y)
		# Type-I JESUp corrected
		vec_u_puppi_TypeI_JESUp = METCorrections.recoil(met_puppi_TypeI_JESUp,vec_boson)
		upar_puppi_TypeI_JESUp = (vec_u_puppi_TypeI_JESUp.x*vec_boson_unit.x) + (vec_u_puppi_TypeI_JESUp.y*vec_boson_unit.y)
		upar_puppi_plus_qt_TypeI_JESUp = upar_puppi_TypeI_JESUp + boson_pt
		upar_puppi_TypeI_JESUp = -upar_puppi_TypeI_JESUp
		response_puppi_TypeI_JESUp = upar_puppi_TypeI_JESUp/boson_pt
		uperp_puppi_TypeI_JESUp = (vec_u_puppi_TypeI_JESUp.y*vec_boson_unit.x) - (vec_u_puppi_TypeI_JESUp.x*vec_boson_unit.y)
		# Type-I JESDown corrected
		vec_u_puppi_TypeI_JESDown = METCorrections.recoil(met_puppi_TypeI_JESDown,vec_boson)
		upar_puppi_TypeI_JESDown = (vec_u_puppi_TypeI_JESDown.x*vec_boson_unit.x) + (vec_u_puppi_TypeI_JESDown.y*vec_boson_unit.y)
		upar_puppi_plus_qt_TypeI_JESDown = upar_puppi_TypeI_JESDown + boson_pt
		upar_puppi_TypeI_JESDown = -upar_puppi_TypeI_JESDown
		response_puppi_TypeI_JESDown = upar_puppi_TypeI_JESDown/boson_pt
		uperp_puppi_TypeI_JESDown = (vec_u_puppi_TypeI_JESDown.y*vec_boson_unit.x) - (vec_u_puppi_TypeI_JESDown.x*vec_boson_unit.y)
	
		# For DY	
		weights = dyJets.dyJetsSelection(events,self.isData)['weights']
		# For Gamma
		#weights = gammaJets.gammaJetsSelection(events)['weights']
	
		out['histo1'].fill(
			dataset=events.metadata["dataset"],
			met_pf_raw_phi=met_pf_raw.phi,
			met_pf_phi=met_pf.phi,
			met_puppi_phi=met_puppi.phi,
			met_puppi_raw_phi=met_puppi_raw.phi,
			weight=weights,
		)
		out['histo2'].fill(
			dataset=events.metadata["dataset"],
			#met_pf_phi_TypeI=vec_met_pf_TypeI.phi[:,0],
			#met_puppi_phi_TypeI=vec_met_puppi_TypeI.phi[:,0],
			met_pf_phi_TypeI=met_pf_TypeI.phi,
			met_puppi_phi_TypeI=met_puppi_TypeI.phi,
			weight=weights,
		)
		out['histo3'].fill(
			dataset=events.metadata["dataset"],
			#met_pf_phi_TypeI=vec_met_pf_TypeI.phi[:,0],
			#met_puppi_phi_TypeI=vec_met_puppi_TypeI.phi[:,0],
			met_pf_phi_TypeI_JESUp=met_pf_TypeI_JESUp.phi,
			met_puppi_phi_TypeI_JESUp=met_puppi_TypeI_JESUp.phi,
			weight=weights,
		)
		out['histo4'].fill(
			dataset=events.metadata["dataset"],
			#met_pf_phi_TypeI=vec_met_pf_TypeI.phi[:,0],
			#met_puppi_phi_TypeI=vec_met_puppi_TypeI.phi[:,0],
			met_pf_phi_TypeI_JESDown=met_pf_TypeI_JESDown.phi,
			met_puppi_phi_TypeI_JESDown=met_puppi_TypeI_JESDown.phi,
			weight=weights,
		)
		out['histo5'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_pf_raw=met_pf_raw.pt,
			#response_pf_raw=response_pf_raw[:,0],
			response_pf_raw=response_pf_raw,
			weight=weights,
		)
		out['histo6'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_puppi_raw=met_puppi_raw.pt,
			#response_puppi_raw=response_puppi_raw[:,0],
			response_puppi_raw=response_puppi_raw,
			weight=weights,
		)
		out['histo7'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_pf=met_pf.pt,
			#response_pf=response_pf[:,0],
			response_pf=response_pf,
			weight=weights,
		)
		out['histo8'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_pf_TypeI=vec_met_pf_TypeI.pt[:,0],
			#response_pf_TypeI=response_pf_TypeI[:,0],
			met_pf_TypeI=met_pf_TypeI.pt,
			response_pf_TypeI=response_pf_TypeI,
			weight=weights,
		)
		out['histo9'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_pf_TypeI=vec_met_pf_TypeI.pt[:,0],
			#response_pf_TypeI=response_pf_TypeI[:,0],
			met_pf_TypeI_JESUp=met_pf_TypeI_JESUp.pt,
			response_pf_TypeI_JESUp=response_pf_TypeI_JESUp,
			weight=weights,
		)
		out['histo10'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_pf_TypeI=vec_met_pf_TypeI.pt[:,0],
			#response_pf_TypeI=response_pf_TypeI[:,0],
			met_pf_TypeI_JESDown=met_pf_TypeI_JESDown.pt,
			response_pf_TypeI_JESDown=response_pf_TypeI_JESDown,
			weight=weights,
		)
		out['histo11'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			met_puppi=met_puppi.pt,
			#response_puppi=response_puppi[:,0],
			response_puppi=response_puppi,
			weight=weights,
		)
		out['histo12'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_puppi_TypeI=vec_met_puppi_TypeI.pt[:,0],
			#response_puppi_TypeI=response_puppi_TypeI[:,0],
			met_puppi_TypeI=met_puppi_TypeI.pt,
			response_puppi_TypeI=response_puppi_TypeI,
			weight=weights,
		)
		out['histo13'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_puppi_TypeI=vec_met_puppi_TypeI.pt[:,0],
			#response_puppi_TypeI=response_puppi_TypeI[:,0],
			met_puppi_TypeI_JESUp=met_puppi_TypeI_JESUp.pt,
			response_puppi_TypeI_JESUp=response_puppi_TypeI_JESUp,
			weight=weights,
		)
		out['histo14'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#met_puppi_TypeI=vec_met_puppi_TypeI.pt[:,0],
			#response_puppi_TypeI=response_puppi_TypeI[:,0],
			met_puppi_TypeI_JESDown=met_puppi_TypeI_JESDown.pt,
			response_puppi_TypeI_JESDown=response_puppi_TypeI_JESDown,
			weight=weights,
		)
		out['histo15'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#upar_pf_raw_plus_qt=upar_pf_raw_plus_qt[:,0],
			#upar_puppi_raw_plus_qt=upar_puppi_raw_plus_qt[:,0],
			upar_pf_raw_plus_qt=upar_pf_raw_plus_qt,
			upar_puppi_raw_plus_qt=upar_puppi_raw_plus_qt,
			weight=weights,
		)
		out['histo16'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#upar_pf_plus_qt=upar_pf_plus_qt[:,0],
			#upar_pf_plus_qt_TypeI=upar_pf_plus_qt_TypeI[:,0],
			upar_pf_plus_qt=upar_pf_plus_qt,
			upar_pf_plus_qt_TypeI=upar_pf_plus_qt_TypeI,
			weight=weights,
		)
		out['histo17'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#upar_pf_plus_qt=upar_pf_plus_qt[:,0],
			#upar_pf_plus_qt_TypeI=upar_pf_plus_qt_TypeI[:,0],
			upar_pf_plus_qt_TypeI_JESUp=upar_pf_plus_qt_TypeI_JESUp,
			upar_pf_plus_qt_TypeI_JESDown=upar_pf_plus_qt_TypeI_JESDown,
			weight=weights,
		)
		out['histo18'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#upar_puppi_plus_qt=upar_puppi_plus_qt[:,0],
			#upar_puppi_plus_qt_TypeI=upar_puppi_plus_qt_TypeI[:,0],
			upar_puppi_plus_qt=upar_puppi_plus_qt,
			upar_puppi_plus_qt_TypeI=upar_puppi_plus_qt_TypeI,
			weight=weights,
		)
		out['histo19'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#upar_puppi_plus_qt=upar_puppi_plus_qt[:,0],
			#upar_puppi_plus_qt_TypeI=upar_puppi_plus_qt_TypeI[:,0],
			upar_puppi_plus_qt_TypeI_JESUp=upar_puppi_plus_qt_TypeI_JESUp,
			upar_puppi_plus_qt_TypeI_JESDown=upar_puppi_plus_qt_TypeI_JESDown,
			weight=weights,
		)
		out['histo20'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#uperp_pf_raw=uperp_pf_raw[:,0],
			#uperp_puppi_raw=uperp_puppi_raw[:,0],
			uperp_pf_raw=uperp_pf_raw,
			uperp_puppi_raw=uperp_puppi_raw,
			weight=weights,
		)
		out['histo21'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#uperp_pf=uperp_pf[:,0],
			#uperp_pf_TypeI=uperp_pf_TypeI[:,0],
			uperp_pf=uperp_pf,
			uperp_pf_TypeI=uperp_pf_TypeI,
			weight=weights,
		)
		out['histo22'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#uperp_pf=uperp_pf[:,0],
			#uperp_pf_TypeI=uperp_pf_TypeI[:,0],
			uperp_pf_TypeI_JESUp=uperp_pf_TypeI_JESUp,
			uperp_pf_TypeI_JESDown=uperp_pf_TypeI_JESDown,
			weight=weights,
		)
		out['histo23'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#uperp_puppi=uperp_puppi[:,0],
			#uperp_puppi_TypeI=uperp_puppi_TypeI[:,0],
			uperp_puppi=uperp_puppi,
			uperp_puppi_TypeI=uperp_puppi_TypeI,
			weight=weights,
		)
		out['histo24'].fill(
			dataset=events.metadata["dataset"],
			qt=boson_pt,
			pv=pv.npvs,
			#uperp_puppi=uperp_puppi[:,0],
			#uperp_puppi_TypeI=uperp_puppi_TypeI[:,0],
			uperp_puppi_TypeI_JESUp=uperp_puppi_TypeI_JESUp,
			uperp_puppi_TypeI_JESDown=uperp_puppi_TypeI_JESDown,
			weight=weights,
		)
		out['histo25'].fill(
			nEventsTot=nEventsTot
		)
		out['histo26'].fill(
			nEventsLumi=nEventsLumi
		)
		out['histo27'].fill(
			nEventsFilters=nEventsFilters
		)
		out['histo28'].fill(
			nEventsTriggers=nEventsTriggers
		)
		out['histo29'].fill(
			nEventsNPV=nEventsNPV
		)
		# For Dy
		out['histo30'].fill(
			nEventsMuon=nEventsMuon
		)
		# For Gamma
		#out['histo20'].fill(
		#	nEventsPhoton=nEventsPhoton
		#)
		#out['histo21'].fill(
		#	nEventsLepton=nEventsLepton
		#)
		#out['histo22'].fill(
		#	nEventsJet=nEventsJet
		#)
		#out['histo23'].fill(
		#	nEventsDeltaR=nEventsDeltaR
		#)
		
		return out
	
	def postprocess(self, accumulator):
		return accumulator
