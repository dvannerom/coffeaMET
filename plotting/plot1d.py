import ROOT
from ROOT import gROOT
from array import array
import math
import argparse
import CMS_lumi, tdrstyle

def varName(var):
	if var == 'qt': return "q_{T} (GeV)"
	elif var == 'met_pf_raw': return "Raw PF p_{T}^{miss} (GeV)"
	elif var == 'met_pf': return "PF p_{T}^{miss} (GeV)"
	elif var == 'met_puppi_raw': return "Raw Puppi p_{T}^{miss} (GeV)"
	elif var == 'met_puppi': return "Puppi p_{T}^{miss} (GeV)"
	elif var == 'met_pf_raw_phi': return "#phi(Raw PF p_{T}^{miss})"
	elif var == 'met_pf_phi': return "#phi(PF p_{T}^{miss})"
	elif var == 'met_puppi_raw_phi': return "#phi(Raw Puppi p_{T}^{miss})"
	elif var == 'met_puppi_phi': return "#phi(Puppi p_{T}^{miss})"
	elif var == 'uperp_raw': return "Raw u_{#perp}  (GeV)"
	elif var == 'uperp_pf': return "PF u_{#perp}  (GeV)"
	elif var == 'uperp_puppi': return "Puppi u_{#perp}  (GeV)"
	elif var == 'upar_raw_plus_qt': return "Raw u_{#parallel} + q_{T} (GeV)"
	elif var == 'upar_pf_plus_qt': return "PF u_{#parallel} + q_{T} (GeV)"
	elif var == 'upar_puppi_plus_qt': return "Puppi u_{#parallel} + q_{T} (GeV)"
	else: print("Not a valid variable")

def yName(var):
	if var == 'qt': return "Events / GeV"
	elif var == 'met_pf_raw': return "Events / GeV"
	elif var == 'met_pf': return "Events / GeV"
	elif var == 'met_puppi_raw': return "Events / GeV"
	elif var == 'met_puppi': return "Events / GeV"
	elif var == 'met_raw_phi': return "Events / 0.16"
	elif var == 'met_pf_phi': return "Events / 0.16"
	elif var == 'met_puppi_phi': return "Events / 0.16"
	elif var == 'uperp_raw': return "Events / 8 GeV"
	elif var == 'uperp_pf': return "Events / 8 GeV"
	elif var == 'uperp_puppi': return "Events / 8 GeV"
	elif var == 'upar_raw_plus_qt': return "Events / 4 GeV"
	elif var == 'upar_pf_plus_qt': return "Events / 4 GeV"
	elif var == 'upar_puppi_plus_qt': return "Events / 4 GeV"
	else: print("Not a valid variable")

def xsec(inFile):
	if inFile == 'GJets_HT-40To100.root': return 18540.0
	elif inFile == 'GJets_HT-100To200.root': return 8644.0
	elif inFile == 'GJets_HT-200To400.root': return 2183.0
	elif inFile == 'GJets_HT-400To600.root': return 260.2
	elif inFile == 'QCD_HT-100To200.root': return 23590000.0
	elif inFile == 'QCD_HT-200To300.root': return 1555000.0
	elif inFile == 'QCD_HT-300To500.root': return 324500.0
	elif inFile == 'QCD_HT-500To700.root': return 30310.0
	elif inFile == 'QCD_HT-700To1000.root': return 6444.0
	elif inFile == 'QCD_HT-1000To1500.root': return 1127.0
	elif inFile == 'QCD_HT-1500To2000.root': return 109.8
	elif inFile == 'QCD_HT-2000ToInf.root': return 21.98
	elif inFile == 'TTGJets.root': return 3.757
	elif inFile == 'TGJets.root': return 2.997
	elif inFile == 'WGJets_PtG-40To130.root': return 19.75
	elif inFile == 'WGJets_PtG-130.root': return 0.8099
	elif inFile == 'WJetsToLNu.root': return 67350.0
	elif inFile == 'DY.root': return 6077.22
	elif inFile == 'TT.root': return 88.29 
	elif inFile == 'WW.root': return 118.7
	elif inFile == 'WZ.root': return 22.82
	elif inFile == 'ZZ.root': return 10.32
	else: print('Invalid sample')

def histoMC(inFileName,var):
	inFile = File = ROOT.TFile.Open(inFileName,'READ')
	histo = inFile.Get(var)
	histo.Sumw2()
	nEvents = (inFile.Get("nEvents")).GetEntries()
	histo.SetDirectory(0)
	inFile.Close()
	#histo.Scale(xsec(inFileName.split('/')[1])/nEvents)
	histo.Scale(xsec(inFileName.split('/')[2])/nEvents)
	return histo

# Define input arguments
parser = argparse.ArgumentParser(description='Plot the selected quantity for the selected particle')
parser.add_argument('-i', '--inputFile', type=str, default='', help='Input data file')
parser.add_argument('-v', '--var', type=str, default='', help='Variable to be plotted')
args = parser.parse_args()

inputFile = args.inputFile
var = args.var

### Define histograms
# Data
dataFile = ROOT.TFile.Open(inputFile,"READ")
h_data = dataFile.Get(var)
### Bkg for GJets
## GJets
#h_GJets_HT40To100 = histoMC('results/GJets_HT-40To100.root',var)
#h_GJets_HT100To200 = histoMC('results/GJets_HT-100To200.root',var)
#h_GJets_HT200To400 = histoMC('results/GJets_HT-200To400.root',var)
#h_GJets_HT400To600 = histoMC('results/GJets_HT-400To600.root',var)
## QCD
#h_QCD_HT100To200 = histoMC('results/QCD_HT-100To200.root',var)
#h_QCD_HT200To300 = histoMC('results/QCD_HT-200To300.root',var)
#h_QCD_HT300To500 = histoMC('results/QCD_HT-300To500.root',var)
#h_QCD_HT500To700 = histoMC('results/QCD_HT-500To700.root',var)
#h_QCD_HT700To1000 = histoMC('results/QCD_HT-700To1000.root',var)
#h_QCD_HT1000To1500 = histoMC('results/QCD_HT-1000To1500.root',var)
#h_QCD_HT1500To2000 = histoMC('results/QCD_HT-1500To2000.root',var)
#h_QCD_HT2000ToInf = histoMC('results/QCD_HT-2000ToInf.root',var)
## Subdominant BG
#h_TTGJets = histoMC('results/TTGJets.root',var)
#h_TGJets = histoMC('results/TGJets.root',var)
#h_WGJets_PtG40To130 = histoMC('results/WGJets_PtG-40To130.root',var)
#h_WGJets_PtG130 = histoMC('results/WGJets_PtG-130.root',var)
#h_WJetsToLNu = histoMC('results/WJetsToLNu.root',var)
#
#### Define summed histograms
## GJets
#h_GJets = h_GJets_HT40To100.Clone("GJets")
#h_GJets.SetDirectory(0)
#h_GJets.Add(h_GJets_HT100To200)
#h_GJets.Add(h_GJets_HT200To400)
#h_GJets.Add(h_GJets_HT400To600)
## QCD
#h_QCD = h_QCD_HT100To200.Clone()
#h_QCD.SetDirectory(0)
#h_QCD.Add(h_QCD_HT200To300)
#h_QCD.Add(h_QCD_HT300To500)
#h_QCD.Add(h_QCD_HT500To700)
#h_QCD.Add(h_QCD_HT700To1000)
#h_QCD.Add(h_QCD_HT1000To1500)
#h_QCD.Add(h_QCD_HT1500To2000)
#h_QCD.Add(h_QCD_HT2000ToInf)
## Sum top and V
#h_TopV = h_TTGJets.Clone()
#h_TopV.SetDirectory(0)
#h_TopV.Add(h_TGJets)
#h_TopV.Add(h_WGJets_PtG40To130)
#h_TopV.Add(h_WGJets_PtG130)

### Bkg for DY
# DY
h_DY = histoMC('results/2022/DY.root',var)
# TT
h_TT = histoMC('results/2022/TT.root',var)
# VV
h_WW = histoMC('results/2022/WW.root',var)
h_WZ = histoMC('results/2022/WZ.root',var)
h_ZZ = histoMC('results/2022/ZZ.root',var)
h_VV = h_WW.Clone("VV")
h_VV.SetDirectory(0)
h_VV.Add(h_WW)
h_VV.Add(h_WZ)
h_VV.Add(h_ZZ)

# Drawing
tdrstyle.setTDRStyle()

#change the CMS_lumi variables (see CMS_lumi.py)
CMS_lumi.lumi_13TeV = "2.94 fb^{-1}" # 2022C
#CMS_lumi.lumi_13TeV = "1.82 fb^{-1}" # 2022D
CMS_lumi.writeExtraText = 1
CMS_lumi.extraText = "Preliminary"
CMS_lumi.lumi_sqrtS = "13 TeV" # used with iPeriod = 0, e.g. for simulation-only plots (default is an empty string)

iPos = 11

iPeriod = 4

H_ref = 700
W_ref = 800
W = W_ref
H  = H_ref

# references for T, B, L, R
T = 0.08*H_ref
B = 0.12*H_ref
L = 0.12*W_ref
R = 0.04*W_ref

canvas = ROOT.TCanvas("c2","c2",50,50,W,H)
canvas.SetFillColor(0)
canvas.SetBorderMode(0)
canvas.SetFrameFillStyle(0)
canvas.SetFrameBorderMode(0)
canvas.Draw()

pad1 = ROOT.TPad("pad1", "pad1", 0, 0.3, 1, 1)
pad1.Draw()
pad1.SetLeftMargin( L/W )
pad1.SetRightMargin( R/W )
pad1.SetTopMargin( T/H )
pad1.SetBottomMargin(0.02);
pad1.SetTickx(0)
pad1.SetTicky(0)
pad1.Draw();
pad1.cd();

#ROOT.gPad.SetLogx()
ROOT.gPad.SetLogy()

h1 = ROOT.TH1F("h1","h1;;"+yName(var),h_data.GetNbinsX(),h_data.GetBinLowEdge(1),h_data.GetBinLowEdge(h_data.GetNbinsX())+h_data.GetBinWidth(h_data.GetNbinsX()))
h1.Draw()

xAxis = h1.GetXaxis()
xAxis.SetNdivisions(6,5,0)
xAxis.SetLabelSize(0)

yAxis = h1.GetYaxis()
yAxis.SetNdivisions(6,5,0)
yAxis.SetTitleOffset(1)

#h_MC_tot = h_GJets.Clone()
#h_MC_tot.SetDirectory(0)
#h_MC_tot.Add(h_QCD)
#h_MC_tot.Add(h_WJetsToLNu)
#h_MC_tot.Add(h_TopV)
#print('% of GJets = '+str(h_GJets.Integral()/h_MC_tot.Integral()))
#print('% of QCD = '+str(h_QCD.Integral()/h_MC_tot.Integral()))
#print('% of WJetsToLNu = '+str(h_WJetsToLNu.Integral()/h_MC_tot.Integral()))
#print('% of TopV = '+str(h_TopV.Integral()/h_MC_tot.Integral()))

h_MC_tot = h_DY.Clone()
h_MC_tot.SetDirectory(0)
h_MC_tot.Add(h_TT)
h_MC_tot.Add(h_VV)
print('% of DY = '+str(h_DY.Integral()/h_MC_tot.Integral()))
print('% of TT = '+str(h_TT.Integral()/h_MC_tot.Integral()))
print('% of VV = '+str(h_VV.Integral()/h_MC_tot.Integral()))

#lumi = 7576.262111 # pb^-1
#h_GJets.SetBinContent(h_GJets.GetNbinsX(),h_GJets.GetBinContent(h_GJets.GetNbinsX())+h_GJets.GetBinContent(h_GJets.GetNbinsX()+1))
#h_GJets.SetBinContent(1,h_GJets.GetBinContent(1)+h_GJets.GetBinContent(0))
##h_GJets.Scale(h_data.Integral()/h_MC_tot.Integral(),"width")
#h_GJets.Scale(lumi,"width")
#h_GJets.SetFillColor(ROOT.kYellow - 7)
#h_GJets.SetLineColor(ROOT.kBlack)
#h_QCD.SetBinContent(h_QCD.GetNbinsX(),h_QCD.GetBinContent(h_QCD.GetNbinsX())+h_QCD.GetBinContent(h_QCD.GetNbinsX()+1))
#h_QCD.SetBinContent(1,h_QCD.GetBinContent(1)+h_QCD.GetBinContent(0))
#h_QCD.Scale(lumi,"width")
#h_QCD.SetFillColor(ROOT.kRed - 7)
#h_QCD.SetLineColor(ROOT.kBlack)
#h_TopV.SetBinContent(h_TopV.GetNbinsX(),h_TopV.GetBinContent(h_TopV.GetNbinsX())+h_TopV.GetBinContent(h_TopV.GetNbinsX()+1))
#h_TopV.SetBinContent(1,h_TopV.GetBinContent(1)+h_TopV.GetBinContent(0))
#h_TopV.Scale(lumi,"width")
#h_TopV.SetFillColor(ROOT.kCyan)
#h_TopV.SetLineColor(ROOT.kBlack)
#h_WJetsToLNu.SetBinContent(h_WJetsToLNu.GetNbinsX(),h_WJetsToLNu.GetBinContent(h_WJetsToLNu.GetNbinsX())+h_WJetsToLNu.GetBinContent(h_WJetsToLNu.GetNbinsX()+1))
#h_WJetsToLNu.SetBinContent(1,h_WJetsToLNu.GetBinContent(1)+h_WJetsToLNu.GetBinContent(0))
#h_WJetsToLNu.Scale(lumi,"width")
#h_WJetsToLNu.SetFillColor(ROOT.kGreen - 7)
#h_WJetsToLNu.SetLineColor(ROOT.kBlack)
#h_stack = ROOT.THStack("h_stack","h_stack")
#h_stack.Add(h_TopV)
#h_stack.Add(h_WJetsToLNu)
#h_stack.Add(h_QCD)
#h_stack.Add(h_GJets)
#h_stack.Draw("histsame")

#lumi = 1821.504603 # pb^-1
lumi = 2938.485599 # pb^-1
h_DY.SetBinContent(h_DY.GetNbinsX(),h_DY.GetBinContent(h_DY.GetNbinsX())+h_DY.GetBinContent(h_DY.GetNbinsX()+1))
h_DY.SetBinContent(1,h_DY.GetBinContent(1)+h_DY.GetBinContent(0))
#h_DY.Scale(h_data.Integral()/h_MC_tot.Integral(),"width")
h_DY.Scale(lumi,"width")
h_DY.SetFillColor(ROOT.kBlue)
h_DY.SetLineColor(ROOT.kBlack)
h_VV.SetBinContent(h_VV.GetNbinsX(),h_VV.GetBinContent(h_VV.GetNbinsX())+h_VV.GetBinContent(h_VV.GetNbinsX()+1))
h_VV.SetBinContent(1,h_VV.GetBinContent(1)+h_VV.GetBinContent(0))
#h_VV.Scale(h_data.Integral()/h_MC_tot.Integral(),"width")
h_VV.Scale(lumi,"width")
h_VV.SetFillColor(ROOT.kYellow)
h_VV.SetLineColor(ROOT.kBlack)
h_TT.SetBinContent(h_TT.GetNbinsX(),h_TT.GetBinContent(h_TT.GetNbinsX())+h_TT.GetBinContent(h_TT.GetNbinsX()+1))
h_TT.SetBinContent(1,h_TT.GetBinContent(1)+h_TT.GetBinContent(0))
#h_TT.Scale(h_data.Integral()/h_MC_tot.Integral(),"width")
h_TT.Scale(lumi,"width")
h_TT.SetFillColor(ROOT.kRed)
h_TT.SetLineColor(ROOT.kBlack)
h_stack = ROOT.THStack("h_stack","h_stack")
h_stack.Add(h_TT)
h_stack.Add(h_VV)
h_stack.Add(h_DY)
h_stack.Draw("histsame")

# Recompute total MC for ratio plot
#h_MC_tot_rescaled = h_GJets.Clone()
#h_MC_tot_rescaled.SetDirectory(0)
#h_MC_tot_rescaled.Add(h_QCD)
#h_MC_tot_rescaled.Add(h_WJetsToLNu)
#h_MC_tot_rescaled.Add(h_TopV)
h_MC_tot_rescaled = h_DY.Clone()
h_MC_tot_rescaled.SetDirectory(0)
h_MC_tot_rescaled.Add(h_TT)
h_MC_tot_rescaled.Add(h_VV)

h_data.SetBinContent(h_data.GetNbinsX(),h_data.GetBinContent(h_data.GetNbinsX())+h_data.GetBinContent(h_data.GetNbinsX()+1))
h_data.SetBinContent(1,h_data.GetBinContent(1)+h_data.GetBinContent(0))
h_data.Scale(1,"width")
h_data.Draw("same")
h_data.SetLineColor(ROOT.kBlack)
h_data.SetMarkerColor(ROOT.kBlack)
h_data.SetMarkerStyle(20)

h1.SetMaximum(30*h_data.GetMaximum())
#h1.SetMinimum(0.1*h_data.GetMinimum())
#h1.SetMinimum(h_TopV.GetMinimum())
h1.SetMinimum(max(1e-02,h_VV.GetMinimum()))

#draw the lumi text on the canvas
CMS_lumi.CMS_lumi(pad1, iPeriod, iPos)

pad1.cd()
pad1.Update()
pad1.RedrawAxis()
frame = pad1.GetFrame()
frame.Draw()

leg = ROOT.TLegend(.75,.55,.9,.9)
leg.SetBorderSize(0)
leg.SetFillColor(0)
leg.SetFillStyle(0)
leg.SetTextFont(42)
leg.SetTextSize(0.04)
#leg.SetHeader("2016G","C");
leg.AddEntry(h_data,"Data","lep")
#leg.AddEntry(h_GJets,"#gamma + jets","f")
#leg.AddEntry(h_QCD,"QCD","f")
#leg.AddEntry(h_WJetsToLNu,"W(l#nu) + jets","f")
#leg.AddEntry(h_TopV,"Top + #gamma, V + #gamma","f")
leg.AddEntry(h_DY,"Drell-Yan","f")
leg.AddEntry(h_VV,"VV","f")
leg.AddEntry(h_TT,"t#bar{t}","f")
leg.Draw()

canvas.cd()

pad2 = ROOT.TPad("pad2", "newpad",0,0,1,0.3);
pad2.Draw();
pad2.cd();
pad2.SetLeftMargin( L/W )
pad2.SetRightMargin( R/W )
pad2.SetTopMargin(0.03);
pad2.SetBottomMargin(0.4);
pad2.SetFillStyle(0);

#ROOT.gPad.SetLogx()

h2 = ROOT.TH1F("h2","h2;"+varName(var)+";Data/MC",h_data.GetNbinsX(),h_data.GetBinLowEdge(1),h_data.GetBinLowEdge(h_data.GetNbinsX())+h_data.GetBinWidth(h_data.GetNbinsX()))
h2.Draw()

xAxis = h2.GetXaxis()
xAxis.SetNdivisions(6,5,0)
xAxis.SetLabelFont(42)
xAxis.SetLabelOffset(0.007)
xAxis.SetLabelSize(0.12)
xAxis.SetTitleFont(42)
xAxis.SetTitleSize(0.14)
xAxis.SetTitleOffset(1.2)

yAxis = h2.GetYaxis()
yAxis.SetNdivisions(6,5,0)
yAxis.SetTitleOffset(0.4)
yAxis.SetLabelFont(42)
#yAxis.SetLabelOffset(0.007)
yAxis.SetLabelSize(0.1)
yAxis.SetTitleFont(42)
yAxis.SetTitleSize(0.12)

h_ratio = h_data.Clone()
h_ratio.SetDirectory(0)
h_ratio.Divide(h_MC_tot_rescaled)
h_ratio.Draw("same")
h_ratio.SetLineColor(ROOT.kBlack)
h_ratio.SetMarkerColor(ROOT.kBlack)
h_ratio.SetMarkerStyle(20)
h2.SetMinimum(0)
h2.SetMaximum(2)

line = ROOT.TLine(h_data.GetXaxis().GetXmin(),1.,h_data.GetXaxis().GetXmax(),1.)
line.SetLineColor(ROOT.kBlack)
line.Draw("same")

canvas.SaveAs('plots/'+var+'.png')
canvas.SaveAs('plots/'+var+'.pdf')
