import ROOT
import array
from ROOT import gROOT
from array import array
import math
import CMS_lumi, tdrstyle
import argparse
from response import response

def resolution(histo):
	nBins = histo.GetNbinsX()
	x, y = array( 'd' ), array( 'd' )
	xerr, yerr = array( 'd' ), array( 'd' )
	for i in range(histo.GetNbinsX()):
		histo_Y = histo.ProjectionY("histo_Y",i,i,"e")
		xaxis = histo.GetXaxis()
		x.append(xaxis.GetBinCenter(i+1))
		xerr.append(xaxis.GetBinWidth(i+1)/2)
		y.append(histo_Y.GetStdDev())
		yerr.append(histo_Y.GetStdDevError())

	return(ROOT.TGraphErrors(nBins,x,y,xerr,yerr))

def resolutionCorr(histo, response_graph):
	nBins = histo.GetNbinsX()
	x, y = array( 'd' ), array( 'd' )
	xerr, yerr = array( 'd' ), array( 'd' )
	for i in range(histo.GetNbinsX()):
		histo_Y = histo.ProjectionY("histo_Y",i,i,"e")
		xaxis = histo.GetXaxis()
		x.append(xaxis.GetBinCenter(i+1))
		xerr.append(xaxis.GetBinWidth(i+1)/2)
		response = response_graph.Eval(xaxis.GetBinCenter(i+1))
		if response > 0:
			y.append(histo_Y.GetStdDev()/response_graph.Eval(xaxis.GetBinCenter(i+1)))
			if histo_Y.GetStdDev() > 0: yerr.append(math.sqrt(math.pow(histo_Y.GetStdDevError()/histo_Y.GetStdDev(),2)+math.pow(response_graph.GetErrorY(i)/response,2))*y[i])
			else: yerr.append(0)
		else:
			y.append(histo_Y.GetStdDev())
			yerr.append(histo_Y.GetStdDevError())

	return(ROOT.TGraphErrors(nBins,x,y,xerr,yerr))

# Define input arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('-i', '--inFileName', type=str, default='', help='Input json file')
parser.add_argument('-c', '--component', type=str, default='upar', help='Parallel or perpendicular component')
parser.add_argument('-v', '--variable', type=str, default='qt', help='qt or pv')
parser.add_argument('--isCorr', action='store_true')
args = parser.parse_args()

inFileName = args.inFileName
component = args.component
variable = args.variable
isCorr = args.isCorr

inFile = ROOT.TFile.Open(inFileName,"READ")

h_pf_raw = inFile.Get(component+"_pf_raw_vs_"+variable)
h_pf = inFile.Get(component+"_pf_vs_"+variable)
h_puppi_raw = inFile.Get(component+"_puppi_raw_vs_"+variable)
h_puppi = inFile.Get(component+"_puppi_vs_"+variable)

# Get response to correct resolutions
h_response_pf_raw = inFile.Get("response_pf_raw_vs_"+variable)
response_pf_raw = response(h_response_pf_raw)
h_response_pf = inFile.Get("response_pf_vs_"+variable)
response_pf = response(h_response_pf)
h_response_puppi_raw = inFile.Get("response_puppi_raw_vs_"+variable)
response_puppi_raw = response(h_response_puppi_raw)
h_response_puppi = inFile.Get("response_puppi_vs_"+variable)
response_puppi = response(h_response_puppi)

resolutionUncorr_pf_raw = resolution(h_pf_raw)
resolutionUncorr_pf = resolution(h_pf)
resolutionUncorr_puppi_raw = resolution(h_puppi_raw)
resolutionUncorr_puppi = resolution(h_puppi)

resolutionCorr_pf_raw = resolutionCorr(h_pf_raw,response_pf_raw)
resolutionCorr_pf = resolutionCorr(h_pf,response_pf)
resolutionCorr_puppi_raw = resolutionCorr(h_puppi_raw,response_puppi_raw)
resolutionCorr_puppi = resolutionCorr(h_puppi,response_puppi)

resolution_pf_raw = resolutionUncorr_pf_raw
resolution_pf = resolutionUncorr_pf
resolution_puppi_raw = resolutionUncorr_puppi_raw
resolution_puppi = resolutionUncorr_puppi
if isCorr:
	resolution_pf_raw = resolutionCorr_pf_raw
	resolution_pf = resolutionCorr_pf
	resolution_puppi_raw = resolutionCorr_puppi_raw
	resolution_puppi = resolutionCorr_puppi

# Drawing
tdrstyle.setTDRStyle()

#change the CMS_lumi variables (see CMS_lumi.py)
#CMS_lumi.lumi_13TeV = "7.6 fb^{-1}"
CMS_lumi.lumi_13TeV = "2.94 fb^{-1}"
CMS_lumi.writeExtraText = 1
CMS_lumi.extraText = "Preliminary"
CMS_lumi.lumi_sqrtS = "13 TeV" # used with iPeriod = 0, e.g. for simulation-only plots (default is an empty string)

iPos = 11

iPeriod = 4

H_ref = 650 
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
#canvas.SetLeftMargin( L/W )
canvas.SetLeftMargin( 0.13 )
canvas.SetRightMargin( R/W )
canvas.SetTopMargin( T/H )
canvas.SetBottomMargin( 1.1 * B/H )
#canvas.SetTickx(0)
#canvas.SetTicky(0)

ROOT.gStyle.SetLegendBorderSize(0)
ROOT.gStyle.SetOptStat(0)

xAxis = resolution_pf_raw.GetXaxis()
xAxis.SetNdivisions(6,5,0)

yAxis = resolution_pf_raw.GetYaxis()
yAxis.SetNdivisions(6,5,0)
yAxis.SetTitleOffset(1)

resolution_pf_raw.Draw("APZ")
resolution_pf_raw.SetMarkerStyle(21)
resolution_pf_raw.SetLineColor(ROOT.kBlue)
resolution_pf_raw.SetMarkerColor(ROOT.kBlue)
if variable == "qt": resolution_pf_raw.GetXaxis().SetTitle("q_{T} (GeV)")
elif variable == "pv": resolution_pf_raw.GetXaxis().SetTitle("Number of primary vertices")
if component == "upar": resolution_pf_raw.GetYaxis().SetTitle("#sigma(u_{#parallel}) (GeV)")
elif component == "uperp": resolution_pf_raw.GetYaxis().SetTitle("#sigma(u_{#perp}  ) (GeV)")
#resolution_pf_raw.SetMinimum(10)
resolution_pf_raw.SetMinimum(0)
resolution_pf_raw.SetMaximum(60)

resolution_pf.Draw("PZ")
resolution_pf.SetLineColor(ROOT.kBlue)
resolution_pf.SetMarkerColor(ROOT.kBlue)
resolution_pf.SetMarkerStyle(20)

resolution_puppi_raw.Draw("PZ")
resolution_puppi_raw.SetLineColor(ROOT.kRed)
resolution_puppi_raw.SetMarkerColor(ROOT.kRed)
resolution_puppi_raw.SetMarkerStyle(21)

resolution_puppi.Draw("PZ")
resolution_puppi.SetLineColor(ROOT.kRed)
resolution_puppi.SetMarkerColor(ROOT.kRed)
resolution_puppi.SetMarkerStyle(20)

#draw the lumi text on the canvas
CMS_lumi.CMS_lumi(canvas, iPeriod, iPos)

canvas.cd()
canvas.Update()
canvas.RedrawAxis()
frame = canvas.GetFrame()
frame.Draw()

latex = ROOT.TLatex()
latex.SetTextFont(42)
latex.SetTextSize(0.04)
latex.DrawLatexNDC(.3,.35,"Response corrected")

leg = ROOT.TLegend(.35,.7,.55,.9)
leg.SetBorderSize(0)
leg.SetFillColor(0)
leg.SetFillStyle(0)
leg.SetTextFont(42)
leg.SetTextSize(0.04)
#leg.SetHeader("#gamma+jets (2016G)","C");
leg.AddEntry(resolution_puppi,"Raw Puppi p_{T}^{miss}","lep")
leg.AddEntry(resolution_puppi_raw,"Type-I Puppi p_{T}^{miss}","lep")
leg.AddEntry(resolution_pf,"Type-I PF p_{T}^{miss}","lep")
leg.AddEntry(resolution_pf_raw,"Raw p_{T}^{miss}","lep")
leg.Draw()

canvas.SaveAs("plots/resolution_"+component+"_"+variable+".png")
canvas.SaveAs("plots/resolution_"+component+"_"+variable+".pdf")
