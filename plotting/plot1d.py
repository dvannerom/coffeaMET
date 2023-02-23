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

# Define input arguments
parser = argparse.ArgumentParser(description='Plot the selected quantity for the selected particle')
parser.add_argument('-i', '--inputFile', type=str, default='', help='Input data file')
parser.add_argument('-v', '--var', type=str, default='', help='Variable to be plotted')
args = parser.parse_args()

inputFile = args.inputFile
var = args.var

### Define histograms
dataFile = ROOT.TFile.Open(inputFile,"READ")
h_data = dataFile.Get(var)

# Drawing
tdrstyle.setTDRStyle()

#change the CMS_lumi variables (see CMS_lumi.py)
CMS_lumi.lumi_13TeV = "59.8 fb^{-1}" # 2018
CMS_lumi.writeExtraText = 1
CMS_lumi.extraText = "Preliminary"
CMS_lumi.lumi_sqrtS = "13 TeV" # used with iPeriod = 0, e.g. for simulation-only plots (default is an empty string)

iPos = 11
iPeriod = 4

H_ref = 700
W_ref = 850
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
canvas.SetLeftMargin( L/W )
canvas.SetRightMargin( R/W )
canvas.SetTopMargin( T/H )
canvas.SetBottomMargin( 1.1 * B/H )

ROOT.gStyle.SetLegendBorderSize(0)
ROOT.gStyle.SetOptStat(0)

#ROOT.gPad.SetLogx()
ROOT.gPad.SetLogy()

#h_data = ROOT.TH1F("h_data","h_data;;"+yName(var),h_data.GetNbinsX(),h_data.GetBinLowEdge(1),h_data.GetBinLowEdge(h_data.GetNbinsX())+h_data.GetBinWidth(h_data.GetNbinsX()))
h_data.SetBinContent(h_data.GetNbinsX(),h_data.GetBinContent(h_data.GetNbinsX())+h_data.GetBinContent(h_data.GetNbinsX()+1))
h_data.SetBinContent(1,h_data.GetBinContent(1)+h_data.GetBinContent(0))
h_data.Scale(1,"width")

h_data.Draw()
h_data.SetLineColor(ROOT.kBlack)
h_data.SetMarkerColor(ROOT.kBlack)
h_data.SetMarkerStyle(20)

xAxis = h_data.GetXaxis()
xAxis.SetNdivisions(6,5,0)
xAxis.SetTitle(varName(var))

yAxis = h_data.GetYaxis()
yAxis.SetNdivisions(6,5,0)
yAxis.SetTitleOffset(1)
yAxis.SetTitle(yName(var))

h_data.SetMaximum(50*h_data.GetMaximum())
#h_data.SetMinimum(max(1e-02,h_VV.GetMinimum()))

#draw the lumi text on the canvas
CMS_lumi.CMS_lumi(canvas, iPeriod, iPos)

canvas.cd()
canvas.Update()
canvas.RedrawAxis()
frame = canvas.GetFrame()
frame.Draw()

canvas.SaveAs('plots/'+var+'.png')
canvas.SaveAs('plots/'+var+'.pdf')
