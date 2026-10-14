from coffea import processor
from coffea.nanoevents import BaseSchema
import awkward as ak
from hist import Hist
import uproot

def load_file(txt_file, prefix="root://sbgdcache.in2p3.fr//"):
    with open(txt_file, "r") as f:
        files = [
            line.strip() if line.strip().startswith("root://") else prefix + line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]

    fileset = {
        "sample": files
    }
    
    return fileset

def define_variables(events):
    """
    Définir toutes les variables dérivées nécessaires pour les cuts,
    directement sur le dataset awkward 'events'.
    """

    events["HCAL_prop"] = events.pfHcalEnergy / (events.pfHcalEnergy + events.pfEcalEnergy)
    events["E_calo"] = events.pfHcalEnergy + events.pfEcalEnergy
    events["E_over_P"] = events.E_calo / events.p_track

    events["tracker_hits"] = ak.num(events.path_length, axis=-1)    
    events["DeDx_hits"] = ak.num(events.cluster_size, axis=-1)     

    events["sigmaPt_over_pt2"] = ak.where(
        (events.p_track != 0) &
        (events.ptError != -0.9274) &
        (events.p_track != 10000),
        events.ptError / (events.p_track ** 2),
        0.0
    )
    return events


class FilteredPlotProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):
        # Appliquer ton filtre / définition de variables
        events = define_variables(events)

        
        # print("Exemple Ih (5 events) :", ak.to_list(events.Ih[:5]))

        # Définir le mask
        mask = (
            (events.chi2 < 5) &
            (events.DeDx_hits > 9) &
            (events.dz < 0.3) &
            (events.dxy < 0.1) &
            (events.trackQuality == 1) &
            (events.sigmaPt_over_pt2 < 0.003) &
            (events.track_PFMiniIso_miniRelIsoAll < 0.02) &
            (events.track_genTrackIsoSumPt_dr03 < 15) &
            (events.Ih > 3) &
            (events.pt_pf > 50) &
            (events.particleType == 1) &
            (events.pt_pf_tau > 50) &
            (events.HCAL_prop > 0.75) &
            (events.eta_pf > -1.3) &
            (events.eta_pf < 1.3)
        )

        exclude_fields = [
            # Liste de tous les champs à exclure
            "nbrEvents", "ptTauGamma", "etaTauGamma", "phiTauGamma", "ETauGamma", "dRTauGamma",
            "ptTauNeutralHad", "etaTauNeutralHad", "phiTauNeutralHad", "ETauNeutralHad","dRTauNeutralHad",
            "ptTauPi0", "etaTauPi0", "phiTauPi0", "ETauPi0", "dRTauPi0",
            "ecalHitEnergies", "ecalHitEnergiesErr", "ecalHitTimes", "ecalHitTimesErr","ecalHitEtas", "ecalHitPhis",
            "path_length", "charge","cluster_size", "nbr_staturated_254", "nbr_staturated_255","cluster_clean",
            "triggerTauNames","triggerMETNames","triggerCategories",
            "flagMET250", "flagMET120", "flagMET105IsoTrk", "flagMonoPFMET120",
            "flagDoubleTau35", "flagTau180", "flagIsoMu20Tau27HPS", "flagTau50MET100",
            "pdgId_gen", "pt_gen", "eta_gen", "phi_gen", "match_index", "matchParticle", "deltaR",
            "passMu", "passTau", "passMet"
        ]

        selected = ak.zip({
            field: events[field][mask]
            for field in events.fields
            if field not in exclude_fields
        })

        # Garder les meilleurs particles
        event_mask = ak.num(selected.pt_pf) > 0
        if ak.sum(event_mask) > 0:
            max_Ih_idx = ak.argmax(selected.Ih, axis=1, keepdims=True)
            best_particles = ak.firsts(selected[max_Ih_idx])
            best_particles = best_particles[event_mask]
            best_particles = ak.zip({
                **{field: best_particles[field] for field in best_particles.fields},
                "flagDoubleTau35": events.flagDoubleTau35[event_mask],
                "flagTau50MET100": events.flagTau50MET100[event_mask],
            })
        else:
            best_particles = ak.Array([])

        # print("\n===== APRÈS FILTRAGE =====")
        # print("Nombre d'événements :", len(best_particles))
        # print("Exemple Ih (5 events) :", ak.to_list(best_particles.Ih[:5]))
        # print("Exemple flag (5 events) :", ak.to_list(best_particles.flagDoubleTau35[:5]))

        # Histogramme sur les pt_pf filtrés
        if len(best_particles) > 0:
            pt = best_particles.pt_pf  # <-- plus de ak.flatten
        else:
            pt = []

        h = Hist.new.Reg(50, 0, 200, name="pt", label="pt_pf").Double()
        h.fill(pt=pt)

        return {
            "hist_pt": h
        }

    def postprocess(self, accumulator):
        # Sauvegarder uniquement l'histogramme
        with uproot.recreate("output.root") as f:
            f["h_pt_pf"] = accumulator["hist_pt"]
        return accumulator


"""LOCAL fileset"""
# fileset = {
# "sample_1": ["/opt/sbg/cms/cms_scratch/xmadre/Q18_M1200.root"],
# "sample_2": ["/opt/sbg/cms/cms_scratch/xmadre/Q21_M1200.root"],
# }
"""REMOTE fileset"""
# fileset = {
# "sample": ["root://sbgdcache.in2p3.fr//cms/phedex//store/user/xmadre/SIM_MC_Part/HSCPmchamp_Q-12_M-200_TuneCP2_13TeV_pythia8/Analysis_HSCPmchamp_Q-12_M-200_CodeV6p8/251210_165121/0000/output_1.root"]
# }
# fileset = {
# "sample": ["root://sbgdcache.in2p3.fr//cms/phedex//store/user/xmadre/SIM_MC_Part/DYJetsToLL_0J_MLL_400_800_TuneCP5_13TeV-amcatnloFXFX-pythia8/Analysis_DYJetsToLL_0J_MLL_400_800_CodeV1p2/251216_143852/0000/output_9.root"]
# }
# fileset = {"sample" : ["root://sbgdcache.in2p3.fr//cms/phedex//store/user/xmadre/SIM_MC_Part/DYJetsToLL_0J_MLL_400_800_TuneCP5_13TeV-amcatnloFXFX-pythia8/Analysis_DYJetsToLL_0J_MLL_400_800_CodeV1p2/251216_143852/0000/output_25.root"]}
"""Macro for all files in a dataset used file.py to create the txt file with all the paths"""
fileset = load_file("files.txt")
runner = processor.Runner(
    # executor=processor.IterativeExecutor(),
    executor=processor.FuturesExecutor(compression=None, workers=16),
    schema=BaseSchema,
)

output = runner(
    fileset,
    treename="PFParticleAnalyzer/candTree",  
    processor_instance=FilteredPlotProcessor(),
)

