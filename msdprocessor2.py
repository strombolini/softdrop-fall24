import time

import coffea.processor as processor
from coffea.analysis_tools import PackedSelection, Weights
from coffea.nanoevents import NanoAODSchema, NanoEventsFactory
from coffea.nanoevents.methods import nanoaod

NanoAODSchema.warn_missing_crossrefs = False

import pickle
import re

import awkward as ak
import numpy as np
import pandas as pd
import json
import fastjet
import dask_awkward
import hist.dask as dah
import hist

# Look at ProcessorABC to see the expected methods and what they are supposed to do
class msdProcessor(processor.ProcessorABC):
    def __init__(self, isMC=False):
        ################################
        # INITIALIZE COFFEA PROCESSOR
        ################################

        # Some examples of axes
        pt_axis = hist.axis.Regular(25, 400, 1200, name="pt", label=r"Jet $p_{T}$ [GeV]")
        eta_axis = hist.axis.Regular(100, -6, 6, name="eta", label=r"Jet eta")

        # Ruva can make her own axes
        ## here
        msoftdrop_axis = hist.axis.Regular(25, 0, 400, name="msoftdrop", label=r"Jet msoftdrop")
        n2_axis = hist.axis.Regular(100, -6, 6, name="n2", label=r"Jet n2")

        
        self.make_output = lambda: { 
            # Test histogram; not needed for final analysis but useful to check things are working
            "ExampleHistogram": dah.Hist(
                #pt_axis,
                #eta_axis,
                msoftdrop_axis,
                #n2_axis,
                #storage=hist.storage.Weight()
            ),
        }
        
    def process(self, events):
        
        output = self.make_output()

        ##################
        # OBJECT SELECTION
        ##################

        # For soft drop studies we care about the AK8 jets
        fatjets = events.FatJet
        
        candidatejet = fatjets[(fatjets.pt > 450)
                               & (abs(fatjets.eta) < 2.5)
                               #& fatjets.isTight
                               ]

        # Let's use only one jet
        leadingjets = candidatejet[:,0:1]

        #Flatten the jet, otherwise it is a jagged array
        leadingjets = ak.flatten(leadingjets, axis = 0)

        jetpt = ak.firsts(leadingjets.pt)      
        jeteta = ak.firsts(leadingjets.eta)
        print('leadingjets.msoftdrop')
        print(leadingjets.msoftdrop)
        print('ak.firsts(leadingjets.msoftdrop)')
        print(ak.firsts(leadingjets.msoftdrop))
        print('ak.num(leadingjets.msoftdrop, axis=1)')
        print(ak.num(leadingjets.msoftdrop, axis=1))

        # Auto-calculated softdrop (no args passed)
        
        jetmsoftdrop = ak.flatten(leadingjets.msoftdrop, axis=1)

        pf = ak.flatten(leadingjets.constituents.pf, axis=1)
        #pf = candidatejet.constituents.pf
        jetdef = fastjet.JetDefinition(fastjet.cambridge_algorithm, 0.8)
        cluster = fastjet.ClusterSequence(pf, jetdef)

        #Start calculating softdrop mass with some parameters
        z_beta_softdrop_matrix = cluster.exclusive_jets_softdrop_grooming()      
        #Put a beta here after fixing other problems 
        z_beta_softdrop_matrix_cluster = fastjet.ClusterSequence(z_beta_softdrop_matrix.constituents, jetdef)

        # Calculate Energy Correlator
        n2 = z_beta_softdrop_matrix_cluster.exclusive_jets_energy_correlator(func="nseries", npoint = 2)
        jetn2=n2

        #Calculate msoftdrop with args passed

        calc_jetmsoftdrop = ak.flatten(z_beta_softdrop_matrix.msoftdrop, axis=0)
        print('calc_jetmsoftdrop')
        print(calc_jetmsoftdrop)
        print('dask_awkward.num(calc_jetmsoftdrop, axis = 0)')
        print(dask_awkward.num(calc_jetmsoftdrop, axis = 0))
        

        
    
        ################
        # EVENT WEIGHTS
        ################

        # Ruva can ignore this section -- it is related to how we produce MC simulation
        
        # create a processor Weights object, with the same length as the number of events in the chunk
        #weights = Weights(dask_awkward.num(events, axis=0).compute())
        weights = Weights(size=None, storeIndividual=True)
        output = self.make_output()
        output['sumw'] = ak.sum(events.genWeight)
        weights.add('genweight', events.genWeight)

        ###################
        # FILL HISTOGRAMS
        ###################
        def normalize(val, cut = None):
            if cut is None:
                ar = ak.fill_none(val, np.nan)
                return ar
            else:
                ar = ak.fill_none(val[cut], np.nan)
                return ar

        output['ExampleHistogram'].fill(#pt=normalize(jetpt),
                                        #eta = normalize(jeteta),
                                        msoftdrop = normalize(calc_jetmsoftdrop),
                                        #n2 = normalize(jetn2),
                                        #weight = weights.weight()
                                        )
    
    
        return output

    def postprocess(self, accumulator):
        return accumulator