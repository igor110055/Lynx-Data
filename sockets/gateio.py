import websocket
import pandas as pd
import rel
import logging
from logging import handlers
from sockets.data import Database
import json

rfh = handlers.RotatingFileHandler(filename='logs/Gateio.log',
                                   maxBytes=2.3 * 1024 * 1024, backupCount= 1, 
                                   encoding=None,
                                   delay=0)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[rfh])
uri = "wss://ws.gate.io/v3/"
markets = ["ALEPH_USDT", "OGN_USDT", "HC_USDT", "QNT_USDT", "POOL_USDT", "KGC_USDT", "MCO2_USDT", "HARD_USDT", "FINE_USDT", "REP_USDT", "SBR_USDT", "ALICE3L_USDT", "BAO_USDT", "FALCONS_USDT", "ANT_USDT", "VIDYX_USDT", "HERO_USDT", "SHARE_USDT", "FIN_USDT", "MTV_USDT", "MOO_USDT", "SMTY_USDT", "ORAO_USDT", "SUSD_USDT", "MAN_USDT", "UNDEAD_USDT", "MC_USDT", "VET_USDT", "LYXE_USDT", "SPS_USDT", "WSIENNA_USDT", "NFTX_USDT", "OPUL_USDT", "ICP3L_USDT", "CTT_USDT", "BSV3L_USDT", "FET_USDT", "VELO_USDT", "BACON_USDT", "DUSK_USDT", "MAPE_USDT", "TDROP_USDT", "C983L_USDT", "CZZ_USDT", "SWRV_USDT", "RACA3S_USDT", "DMLG_USDT", "ASM_USDT", "ONC_USDT", "DAI_USDT", "PYM_USDT", "LOKA_USDT", "NIF_USDT", "BNC_USDT", "MATIC3S_USDT", "STMX_USDT", "SKL_USDT", "AMPL3S_USDT", "WEX_USDT", "COTI_USDT", "ORT_USDT", "RACA3L_USDT", "GASDAO_USDT", "AVA_USDT", "OPA_USDT", "ATS_USDT", "VEGA_USDT", "KILT_USDT", "BRISE_USDT", "FSN_USDT", "KYL_USDT", "XRP_USDT", "DYDX3S_USDT", "MANA3S_USDT", "ALICE3S_USDT", "PCX_USDT", "WOO3S_USDT", "MATIC_USDT", "ICP3S_USDT", "CRO3S_USDT", "FTT_USDT", "TAP_USDT", "MLT_USDT", "RBN_USDT", "AMPL3L_USDT", "HECH_USDT", "WOO3L_USDT", "TAI_USDT", "HERA_USDT", "AST_USDT", "XAVA_USDT", "LSS_USDT", "SNX3S_USDT", "PBR_USDT", "PRQ_USDT", "MATIC3L_USDT", "LPOOL_USDT", "PSP_USDT", "BXC_USDT", "CBK_USDT", "MANA3L_USDT", "ALPINE_USDT", "DEGO_USDT", "SIN_USDT", "OCT_USDT", "KZEN_USDT", "L3P_USDT", "BORA_USDT", "NEO3L_USDT", "FROG_USDT", "CHAMP_USDT", "XNFT_USDT", "BCH3S_USDT", "CRPT_USDT", "ROUTE_USDT", "GLM_USDT", "TIMECHRONO_USDT", "VRA_USDT", "ONS_USDT", "ZEC3L_USDT", "FRA_USDT", "BLANK_USDT", "IOST3L_USDT", "DDD_USDT", "UNQ_USDT", "API33S_USDT", "GITCOIN_USDT", "THG_USDT", "FLY_USDT", "CREDIT_USDT", "RENA_USDT", "NBOT_USDT", "HT3L_USDT", "ASD_USDT", "XMR_USDT", "FTM_USDT", "API33L_USDT", "PIG_USDT", "THN_USDT", "BCUG_USDT", "GGM_USDT", "HOTCROSS_USDT", "SKYRIM_USDT", "BTG_USDT", "POT_USDT", "CS_USDT", "XVS_USDT", "A5T_USDT", "WAVES_USDT", "YIN_USDT", "PEOPLE_USDT", "POLC_USDT", "BZZ3L_USDT", "UNO_USDT", "HDV_USDT", "CELL_USDT", "FODL_USDT", "PROS_USDT", "WAG_USDT", "VENT_USDT", "WND_USDT", "ZEC3S_USDT", "DOS_USDT", "HT3S_USDT", "LAND_USDT", "RING_USDT", "FIRO_USDT", "AUDIO_USDT", "KUMA_USDT", "CRBN_USDT", "XMARK_USDT", "SAKE_USDT", "SLP_USDT", "F2C_USDT", "LUNA_USDT", "ONIT_USDT", "FTM3L_USDT", "RFUEL_USDT", "NEO3S_USDT", "MIR_USDT", "EGS_USDT", "DAR_USDT", "KSM_USDT", "QTUM_USDT", "C983S_USDT", "DOGE3S_USDT", "RVN_USDT", "NOS_USDT", "LUNC_USDT", "BZZ3S_USDT", "ONE_USDT", "FTM3S_USDT", "FLUX_USDT", "MNW_USDT", "WAGYU_USDT", "ANGLE_USDT", "DOGA_USDT", "JFI_USDT", "USDG_USDT", "DMTR_USDT", "ENJ_USDT", "GOLDMINER_USDT", "WIT_USDT", "DOGE3L_USDT", "FORM_USDT", "MLK_USDT", "VR_USDT", "DMS_USDT", "ONX_USDT", "ASK_USDT", "TXT_USDT", "NIIFI_USDT", "VRX_USDT", "DOME_USDT", "CTSI_USDT", "ORBS_USDT", "FIL_USDT", "CTK_USDT", "ASR_USDT", "RAM_USDT", "IRIS_USDT", "AME_USDT", "KUB_USDT", "ENV_USDT", "COTI3S_USDT", "ACH3S_USDT", "IHT_USDT", "POLYPAD_USDT", "CTRC_USDT", "SFUND_USDT", "SHIB_USDT", "NFT_USDT", "SOLO_USDT", "TSHP_USDT", "AMP_USDT", "VLXPAD_USDT", "GAN_USDT", "O3_USDT", "TULIP_USDT", "TT_USDT", "SHILL_USDT", "PSB_USDT", "XDC_USDT", "LON3L_USDT", "HE_USDT", "SKU_USDT", "QLC_USDT", "DOMI_USDT", "IDEA_USDT", "METO_USDT", "ACH3L_USDT", "ALGO_USDT", "BLIN_USDT", "RBLS_USDT", "VRT_USDT", "DOT5S_USDT", "GST_USDT", "KIBA_USDT", "CHESS_USDT", "XLM3L_USDT", "LIQ_USDT", "EPK_USDT", "ZCN_USDT", "CVC3L_USDT", "MNY_USDT", "SALT_USDT", "CSTR_USDT", "MPL_USDT", "FIS_USDT", "HIGH_USDT", "ROSE_USDT", "FRAX_USDT", "BAGS_USDT", "WOO_USDT", "SNX3L_USDT", "VTHO_USDT", "OKB3L_USDT", "SAFEMOON_USDT", "IOI_USDT", "LAMB_USDT", "CHZ_USDT", "OKB3S_USDT", "ELU_USDT", "EOS3S_USDT", "DBC_USDT", "ATOM_USDT", "WZRD_USDT", "MEAN_USDT", "IDEX_USDT", "EMON_USDT", "FXS_USDT", "SIDUS_USDT", "ATA_USDT", "CVC3S_USDT", "LON3S_USDT", "INJ_USDT", "MAHA_USDT", "IOST3S_USDT", "VOXEL_USDT", "CRV_USDT", "EQX_USDT", "WHALE_USDT", "GRAP_USDT", "AVAX3S_USDT", "C98_USDT", "VET3S_USDT", "KPAD_USDT", "CRO_USDT", "LEMD_USDT", "PERL_USDT", "RATIO_USDT", "UMB_USDT", "NUM_USDT", "SHOE_USDT", "SDN_USDT", "BRKL_USDT", "ELEC_USDT", "SFG_USDT", "COFIX_USDT", "CWAR_USDT", "WILD_USDT", "RENBTC_USDT", "BNX_USDT", "TRU_USDT", "1EARTH_USDT", "ADAPAD_USDT", "PPS_USDT", "NFTL_USDT", "SHIB3S_USDT", "CNAME_USDT", "ZCX_USDT", "DYDX3L_USDT", "ASTRO_USDT", "GLQ_USDT", "PROPS_USDT", "AART_USDT", "KFT_USDT", "AERGO_USDT", "EOS3L_USDT", "API3_USDT", "LOON_USDT", "AVAX3L_USDT", "VET3L_USDT", "AE_USDT", "LYM_USDT", "LBK_USDT", "QTC_USDT", "LAVA_USDT", "XCN_USDT", "BRT_USDT", "RSV_USDT", "KIF_USDT", "AZERO_USDT", "MILO_USDT", "TOTM_USDT", "MINA_USDT", "TITA_USDT", "COTI3L_USDT", "DAG_USDT", "DOT5L_USDT", "TRADE_USDT", "NU_USDT", "POLS_USDT", "NPT_USDT", "MTA_USDT", "YIELD_USDT", "KART_USDT", "SYLO_USDT", "BASE_USDT", "ICX_USDT", "PET_USDT", "GZONE_USDT", "SBTC_USDT", "NAFT_USDT", "VADER_USDT", "GTC_USDT", "XRPBEAR_USDT", "TIME_USDT", "SXP_USDT", "CITY_USDT", "QASH_USDT", "FAST_USDT", "BCD_USDT", "KNIGHT_USDT", "ZODI_USDT", "REI_USDT", "SRM_USDT", "ZEC_USDT", "UFT_USDT", "RIDE_USDT", "ERN_USDT", "T_USDT", "CEEK_USDT", "STI_USDT", "IMX3S_USDT", "SUPE_USDT", "AR3L_USDT", "QSP_USDT", "FLM_USDT", "AAVE3S_USDT", "BOND_USDT", "TARA_USDT", "TRX_USDT", "SPO_USDT", "DSLA_USDT", "DOGE_USDT", "CFX3S_USDT", "QUICK_USDT", "UTK_USDT", "XPNET_USDT", "TRB_USDT", "LAZIO_USDT", "WSG_USDT", "DASH3L_USDT", "BTL_USDT", "CPOOL_USDT", "REALM_USDT", "ALPHA3S_USDT", "BLOK_USDT", "WIKEN_USDT", "OMG3S_USDT", "BCH5S_USDT", "MED_USDT", "CGG_USDT", "CRE_USDT", "SOURCE_USDT", "ABT_USDT", "DPET_USDT", "WOM_USDT", "RIF_USDT", "BENQI_USDT", "LAT_USDT", "ITGR_USDT", "DLTA_USDT", "SMT_USDT", "APYS_USDT", "STOX_USDT", "GMAT_USDT", "RAZOR_USDT", "RAGE_USDT", "DOCK_USDT", "RDN_USDT", "MTR_USDT", "NKN_USDT", "SWASH_USDT", "FX_USDT", "DERI_USDT", "DFND_USDT", "BLES_USDT", "SLND_USDT", "CRTS_USDT", "BTC3S_USDT", "BKC_USDT", "THETA3L_USDT", "LOOKS_USDT", "ETH3L_USDT", "DOGEDASH_USDT", "QTCON_USDT", "BABI_USDT", "PNL_USDT", "BTCBULL_USDT", "HMT_USDT", "PORTO_USDT", "STND_USDT", "LPT_USDT", "LTC3L_USDT", "TOKAU_USDT", "TVK_USDT", "CWS_USDT", "SWOP_USDT", "WBTC_USDT", "GALA5L_USDT", "AGS_USDT", "CATGIRL_USDT", "GCOIN_USDT", "GDAO_USDT", "PMON_USDT", "MNGO_USDT", "MSOL_USDT", "POWR_USDT", "UOS_USDT", "USDD_USDT", "SLICE_USDT", "NSBT_USDT", "BEAM3L_USDT", "BEL_USDT", "MM_USDT", "FTT3L_USDT", "OMI_USDT", "TIPS_USDT", "SQUID_USDT", "FEI_USDT", "GEM_USDT", "UMEE_USDT", "FCD_USDT", "PVU_USDT", "NRFB_USDT", "LION_USDT", "BLACK_USDT", "DOGE5S_USDT", "CUDOS_USDT", "PCNT_USDT", "OVR_USDT", "ETC3S_USDT", "MER_USDT", "BOBA_USDT", "FUEL_USDT", "BAC_USDT", "ONE3S_USDT", "OPIUM_USDT", "JST3L_USDT", "BONDLY_USDT", "RAZE_USDT", "LDO_USDT", "ORO_USDT", "LITH_USDT", "RLY_USDT", "NEAR3S_USDT", "XLM3S_USDT", "AR_USDT", "AKT_USDT", "HCT_USDT", "BZZ_USDT", "SRM3L_USDT", "AQDC_USDT", "EWT_USDT", "CORN_USDT", "HYDRA_USDT", "STEP_USDT", "MATTER_USDT", "LIKE_USDT", "HIT_USDT", "LEO_USDT", "COMP_USDT", "BAL_USDT", "LMR_USDT", "AQT_USDT", "LINK3S_USDT", "IMX_USDT", "EFI_USDT", "TAUR_USDT", "MOOV_USDT", "RUNE_USDT", "TCP_USDT", "SCLP_USDT", "RBC_USDT", "SPI_USDT", "ETC_USDT", "CHICKS_USDT", "KNOT_USDT", "XEC3L_USDT", "XCV_USDT", "APT_USDT", "KISHU_USDT", "LIEN_USDT", "CREAM_USDT", "ATOM3S_USDT", "PYR_USDT", "METAG_USDT", "ACE_USDT", "CIR_USDT", "TRIBE3S_USDT", "EVA_USDT", "BBANK_USDT", "BLANKV2_USDT", "BAL3S_USDT", "ALTB_USDT", "KNC_USDT", "GAS_USDT", "SAFEMARS_USDT", "TIP_USDT", "NWC_USDT", "VALUE_USDT", "SSX_USDT", "JOE_USDT", "FITFI3S_USDT", "BIT_USDT", "MSU_USDT", "CRV3L_USDT", "OXT_USDT", "SHFT_USDT", "BP_USDT", "KBOX_USDT", "PERP_USDT", "SAO_USDT", "DUCK2_USDT", "DEFILAND_USDT", "GLMR3L_USDT", "MTS_USDT", "STX_USDT", "ZIG_USDT", "CARDS_USDT", "ANML_USDT", "GALA_USDT", "RAY3S_USDT", "KAVA3L_USDT", "GARD_USDT", "GRT3L_USDT", "BFC_USDT", "NIFT_USDT", "ORION_USDT", "CTX_USDT", "ASW_USDT", "CERE_USDT", "MKR_USDT", "MASK_USDT", "MGA_USDT", "AVAX_USDT", "SKL3L_USDT", "FRR_USDT", "MV_USDT", "SFIL_USDT", "TEER_USDT", "KLV_USDT", "MKR3L_USDT", "OIN_USDT", "CAKE_USDT", "RNDR_USDT", "STEPG_USDT", "YCT_USDT", "SHR_USDT", "ONT_USDT", "JASMY3L_USDT", "NFTD_USDT", "MATH_USDT", "DERC_USDT", "FEG_USDT", "ZRX_USDT", "BAND_USDT", "LOA_USDT", "HSF_USDT", "KMON_USDT", "LUNR_USDT", "THETA_USDT", "NBS_USDT", "FRM_USDT", "DEP_USDT", "AUCTION_USDT", "ORC_USDT", "LIME_USDT", "BSCS_USDT", "VAI_USDT", "RVC_USDT", "NULS_USDT", "PEARL_USDT", "COMP3S_USDT", "PHTR_USDT", "OST_USDT", "ALGO3L_USDT", "BFT_USDT", "XY_USDT", "SOUL_USDT", "CPAN_USDT", "DOE_USDT", "STG_USDT", "POOLZ_USDT", "ZIL_USDT", "APE3L_USDT", "DG_USDT", "DENT_USDT", "REN_USDT", "88MPH_USDT", "LOCG_USDT", "CATE_USDT", "BLZ_USDT", "HEGIC_USDT", "DPY_USDT", "EGAME_USDT", "GRIN3L_USDT", "UNI3S_USDT", "FOR_USDT", "SXP3L_USDT", "GSE_USDT", "WEST_USDT", "BTM3S_USDT", "ARRR_USDT", "EURT_USDT", "KST_USDT", "SLP3S_USDT", "ALY_USDT", "WAVES3L_USDT", "RUNE3S_USDT", "BAT_USDT", "RAMP_USDT", "LIFE_USDT", "OCC_USDT", "BMON_USDT", "KINE_USDT", "COOK_USDT", "BDT_USDT", "XAUT_USDT", "MIST_USDT", "FCON_USDT", "ANC_USDT", "UNI5S_USDT", "PEOPLE3S_USDT", "ALCX_USDT", "HBAR3S_USDT", "KTN_USDT", "BEAM_USDT", "STAR_USDT", "GST3L_USDT", "RCN_USDT", "BMI_USDT", "LOWB_USDT", "ENJ3S_USDT", "DILI_USDT", "OM_USDT", "UNN_USDT", "MMM_USDT", "POND_USDT", "EDEN_USDT", "MUSE_USDT", "UNFI_USDT", "ATOLO_USDT", "KLAY3S_USDT", "FORTH_USDT", "CIRUS_USDT", "DFY_USDT", "RLC_USDT", "RON_USDT", "YFDAI_USDT", "SAND3L_USDT", "SENC_USDT", "MTL3L_USDT", "FIRE_USDT", "NEST_USDT", "XRP3S_USDT", "AUTO_USDT", "VSO_USDT", "NIFTSY_USDT", "KOK_USDT", "OCN_USDT", "TAKI_USDT", "DELFI_USDT", "GOFX_USDT", "PLA_USDT", "CYS_USDT", "VEMP_USDT", "SOL3L_USDT", "LTC5L_USDT", "FITFI_USDT", "MIMIR_USDT", "WRX_USDT", "1INCH3S_USDT", "HNT_USDT", "XRP5S_USDT", "DOGGY_USDT", "AVT_USDT", "RAY_USDT", "IOEN_USDT", "FOX_USDT", "ETERNAL_USDT", "CSPR3L_USDT", "BLOCK_USDT", "SFI_USDT", "DEVT_USDT", "LEASH_USDT", "BAKED_USDT", "CFX_USDT", "STRM_USDT", "PNG_USDT", "NBP_USDT", "PRARE_USDT", "TOMO_USDT", "LFW_USDT", "SPELLFIRE_USDT", "PARA_USDT", "DES_USDT", "SB_USDT", "XCH3L_USDT", "OKT_USDT", "HOGE_USDT", "SUSHI3L_USDT", "CAPS_USDT", "HSC_USDT", "STBU_USDT", "OLT_USDT", "NAP_USDT", "ACH_USDT", "GAL3L_USDT", "EMPIRE_USDT", "GTH_USDT", "AKRO_USDT", "CAKE3L_USDT", "MEPAD_USDT", "WNCG_USDT", "CVP_USDT", "MARS_USDT", "BOO_USDT", "ANC3L_USDT", "CRO3L_USDT", "YFII3L_USDT", "ETHBULL_USDT", "WICC_USDT", "METIS_USDT", "ROSE3S_USDT", "FEAR_USDT", "KAVA_USDT", "APN_USDT", "QANX_USDT", "CONV_USDT", "QTUM3S_USDT", "LRC3S_USDT", "TRX3S_USDT", "ROOK_USDT", "PHA_USDT", "ELF_USDT", "WAR_USDT", "ZEUM_USDT", "NAS_USDT", "VIDY_USDT", "HOT_USDT", "LUNA3S_USDT", "DARK_USDT", "ALICE_USDT", "CFG_USDT", "TRVL_USDT", "XCAD_USDT", "UBXS_USDT", "ETHBEAR_USDT", "LTO_USDT", "RIN_USDT", "IOST_USDT", "BNB3L_USDT", "BTO_USDT", "OP_USDT", "ROSN_USDT", "STR_USDT", "MINT_USDT", "XTAG_USDT", "GM_USDT", "DHV_USDT", "SPA_USDT", "BTCST_USDT", "STARL_USDT", "BCDN_USDT", "LINK_USDT", "TLM_USDT", "BCMC_USDT", "RARE_USDT", "MGG_USDT", "PSG_USDT", "LIQUIDUS_USDT", "KEY_USDT", "ALN_USDT", "K21_USDT", "MAPS_USDT", "BSV5L_USDT", "XMR3L_USDT", "KTON_USDT", "ATK_USDT", "LINK5S_USDT", "VEE_USDT", "SASHIMI_USDT", "ARTEM_USDT", "HYVE_USDT", "STSOL_USDT", "OXY_USDT", "BOA_USDT", "SERO3S_USDT", "OG_USDT", "MARSH_USDT", "TWT_USDT", "VGX_USDT", "ARPA_USDT", "GALA3S_USDT", "KINGSHIB_USDT", "WHITE_USDT", "SENATE_USDT", "EGLD3S_USDT", "STRP_USDT", "MIX_USDT", "GARI_USDT", "KMA_USDT", "CHAIN_USDT", "IZI_USDT", "DEPO_USDT", "ONT3S_USDT", "AFC_USDT", "PUNDIX_USDT", "GGG_USDT", "UNI_USDT", "CWEB_USDT", "GMT_USDT", "CSPR_USDT", "BBF_USDT", "POLI_USDT", "QRDO_USDT", "GALA3L_USDT", "OPEN_USDT", "GRT_USDT", "SANDWICH_USDT", "PAF_USDT", "POG_USDT", "ADEL_USDT", "XYM_USDT", "EGLD_USDT", "TSL_USDT", "RMRK_USDT", "GENS_USDT", "MCASH_USDT", "SSV_USDT", "CVC_USDT", "TOOLS_USDT", "FAR_USDT", "BLT_USDT", "DIS_USDT", "MOBI_USDT", "DDOS_USDT", "WNZ_USDT", "JUV_USDT", "ETH5S_USDT", "XDEFI_USDT", "OLYMPUS_USDT", "FEVR_USDT", "MBL_USDT", "MASK3L_USDT", "MQL_USDT", "GALFAN_USDT", "NRV_USDT", "KIN_USDT", "ETH5L_USDT", "KALM_USDT", "DX_USDT", "AXS5S_USDT", "SHI_USDT", "EOSBEAR_USDT", "FXF_USDT", "MASK3S_USDT", "AXL_USDT", "OP3L_USDT", "NOIA_USDT", "DOSE_USDT", "VLX_USDT", "BNB_USDT", "BATH_USDT", "MDX_USDT", "CORAL_USDT", "1ART_USDT", "KT_USDT", "BTC5L_USDT", "OLV_USDT", "POLYDOGE_USDT", "MBS_USDT", "CRP_USDT", "SRP_USDT", "DFA_USDT", "YFI_USDT", "DASH_USDT", "DDIM_USDT", "REQ_USDT", "ZIL3L_USDT", "NYZO_USDT", "CORE_USDT", "INSUR_USDT", "OOKI_USDT", "LRN_USDT", "JGN_USDT", "NEXT_USDT", "LOOKS3S_USDT", "METAX_USDT", "DRGN_USDT", "WAVES3S_USDT", "ZSC_USDT", "NUX_USDT", "NEAR_USDT", "ZOON_USDT", "MTRG_USDT", "MINA3L_USDT", "MRCH_USDT", "TCT_USDT", "FST_USDT", "YFII_USDT", "POLY_USDT", "MMPRO_USDT", "SOL_USDT", "BTC5S_USDT", "GAIA_USDT", "COMBO_USDT", "SCNSOL_USDT", "AXS_USDT", "NFTY_USDT", "FIO_USDT", "LIT_USDT", "ISKY_USDT", "FIL3S_USDT", "OP3S_USDT", "ELON_USDT", "LUFFY_USDT", "LOOKS3L_USDT", "SNOW_USDT", "EVER_USDT", "UNCX_USDT", "TABOO_USDT", "UMX_USDT", "KASTA_USDT", "KBD_USDT", "ZIL3S_USDT", "BTCBEAR_USDT", "SOS_USDT", "ARMOR_USDT", "KONO_USDT", "RITE_USDT", "MDA_USDT", "KSM3L_USDT", "MINI_USDT", "GST3S_USDT", "MINA3S_USDT", "ARPA3L_USDT", "ORAI_USDT", "STOS_USDT", "SERO3L_USDT", "MANA_USDT", "SKRT_USDT", "OKB_USDT", "LINK5L_USDT", "RGT_USDT", "ATD_USDT", "SCY_USDT", "MTN_USDT", "ADS_USDT", "GMT3S_USDT", "COMP3L_USDT", "QKC_USDT", "STORJ_USDT", "XEM_USDT", "LSK_USDT", "PWAR_USDT", "IOTX_USDT", "CVX_USDT", "SNFT_USDT", "GMM_USDT", "PSTAKE_USDT", "NEXO_USDT", "FILDA_USDT", "BORING_USDT", "ETH_USDT", "WNXM_USDT", "FLOKI_USDT", "STORE_USDT", "EGLD3L_USDT", "SFM_USDT", "BU_USDT", "REAP_USDT", "STETH_USDT", "GXS_USDT", "DEK_USDT", "KSM3S_USDT", "CHR_USDT", "ARPA3S_USDT", "CART_USDT", "ZEN3S_USDT", "ALPACA_USDT", "HIVE_USDT", "KP3R_USDT", "METAL_USDT", "MFT_USDT", "GS_USDT", "ZEN3L_USDT", "GMT3L_USDT", "ANKR_USDT", "LIBRE_USDT", "MELI_USDT", "XRUNE_USDT", "SERO_USDT", "RSS3_USDT", "WINGS_USDT", "XCH_USDT", "ORN_USDT", "EOS5S_USDT", "FOREX_USDT", "ZAM_USDT", "BADGER_USDT", "HGET_USDT", "LKR_USDT", "AXS3L_USDT", "ADA3S_USDT", "BAKE_USDT", "ESG_USDT", "SNT_USDT", "NAX_USDT", "DREP_USDT", "NORD_USDT", "MONS_USDT", "JOY_USDT", "BOSON_USDT", "DVP_USDT", "OMG_USDT", "MPH_USDT", "BETU_USDT", "DOP_USDT", "BCN_USDT", "SMG_USDT", "BURP_USDT", "RAI_USDT", "GOD_USDT", "WNDR_USDT", "ZEN_USDT", "SC_USDT", "GUM_USDT", "MOB_USDT", "WALLET_USDT", "DORA_USDT", "DIVER_USDT", "CLH_USDT", "100X_USDT", "DV_USDT", "CRAFT_USDT", "REEF_USDT", "COS_USDT", "PROM_USDT", "ZKT_USDT", "LOKA3S_USDT", "ARNX_USDT", "IPAD_USDT", "AXS3S_USDT", "SKM_USDT", "ASTR3S_USDT", "EOS5L_USDT", "NYM_USDT", "ADA3L_USDT", "STC_USDT", "ZMT_USDT", "DOT_USDT", "MDS_USDT", "CKB_USDT", "STZ_USDT", "CRU_USDT", "PLATO_USDT", "GMEE_USDT", "MAT_USDT", "STEEM_USDT", "BSW_USDT", "BSW3S_USDT", "CNNS_USDT", "BTF_USDT", "ZPT_USDT", "LGCY_USDT", "SOLR_USDT", "LOKA3L_USDT", "OUSD_USDT", "ETHA_USDT", "ALPA_USDT", "FIL3L_USDT", "SWAP_USDT", "ZINU_USDT", "XTZ3S_USDT", "DYP_USDT", "PLACE_USDT", "SAITAMA_USDT", "XTZ3L_USDT", "XLM_USDT", "PBTC35A_USDT", "CPHR_USDT", "PBX_USDT", "ASTR3L_USDT", "BSCPAD_USDT", "DUCK_USDT", "SNX_USDT", "KFC_USDT", "RSR_USDT", "MITH_USDT", "BEEFI_USDT", "WAXP_USDT", "REF_USDT", "AXIS_USDT", "CCAR_USDT", "NAOS_USDT", "XMC_USDT", "ONSTON_USDT", "TRA_USDT", "TIDAL_USDT", "TBE_USDT", "PERA_USDT", "SYS_USDT", "GOLD_USDT", "PING_USDT", "ZLW_USDT", "XED_USDT", "DOT3L_USDT", "ASS_USDT", "ADP_USDT", "TRACE_USDT", "DNXC_USDT", "REVV_USDT", "ALAYA_USDT", "ESD_USDT", "TALK_USDT", "SWFTC_USDT", "URUS_USDT", "YFI3L_USDT", "BAT3L_USDT", "SKT_USDT", "BEYOND_USDT", "SUP_USDT", "RACA_USDT", "PIT_USDT", "ODDZ_USDT", "AXS5L_USDT", "RATING_USDT", "ICONS_USDT", "BTM_USDT", "TKO_USDT", "FLURRY_USDT", "SHIB5L_USDT", "BSW3L_USDT", "CHZ3S_USDT", "VTG_USDT", "LBA_USDT", "CHER_USDT", "ALPH_USDT", "BOX_USDT", "BAMBOO_USDT", "PKF_USDT", "PLSPAD_USDT", "ARV_USDT", "APE_USDT", "WAM_USDT", "FIDA_USDT", "BAT3S_USDT", "TTK_USDT", "ZBC_USDT", "TFUEL_USDT", "LIT3L_USDT", "FARM_USDT", "LRC_USDT", "CHNG_USDT", "EHASH_USDT", "IOTA_USDT", "RFOX_USDT", "IAG_USDT", "CUSD_USDT", "FTRB_USDT", "UPI_USDT", "KEEP_USDT", "SINGLE_USDT", "XRPBULL_USDT", "GAFI_USDT", "LIT3S_USDT", "DOT3S_USDT", "HAI_USDT", "BTC_USDT", "CEL_USDT", "NMT_USDT", "LTC_USDT", "SHIB5S_USDT", "WWY_USDT", "ZONE_USDT", "VDR_USDT", "PICKLE_USDT", "HTR_USDT", "PHM_USDT", "XEND_USDT", "WOOP_USDT", "CLV_USDT", "CELT_USDT", "MIS_USDT", "SLC_USDT", "SWINGBY_USDT", "RICE_USDT", "BTT_USDT", "NFTB_USDT", "YFI3S_USDT", "ONT3L_USDT", "OLY_USDT", "DVI_USDT", "PPAD_USDT", "CHZ3L_USDT", "XCUR_USDT", "CFX3L_USDT", "GNX_USDT", "JASMY_USDT", "FITFI3L_USDT", "ETH3S_USDT", "WOZX_USDT", "ZEE_USDT", "FUN_USDT", "NOA_USDT", "PRIDE_USDT", "METAN_USDT", "LON_USDT", "BTC3L_USDT", "KRL_USDT", "THETA3S_USDT", "TRIBE3L_USDT", "ISP_USDT", "BUY_USDT", "QUACK_USDT", "GRT3S_USDT", "SWTH_USDT", "BICO_USDT", "BIN_USDT", "MKR3S_USDT", "DIGG_USDT", "TROY_USDT", "SENSO_USDT", "BCH_USDT", "KLAY_USDT", "WING_USDT", "MIT_USDT", "OPS_USDT", "SAVG_USDT", "TON_USDT", "AAVE3L_USDT", "KABY_USDT", "DEUS_USDT", "GAME_USDT", "FIC_USDT", "QBT_USDT", "IMX3L_USDT", "ASTR_USDT", "GMPD_USDT", "ORCA_USDT", "HIBIKI_USDT", "ONG_USDT", "ERG_USDT", "ALPHA_USDT", "CELO_USDT", "XVG_USDT", "BTS_USDT", "DIA_USDT", "XPRESS_USDT", "LOOT_USDT", "STAKE_USDT", "XEC_USDT", "PIZA_USDT", "NSDX_USDT", "MOOO_USDT", "EOS_USDT", "SNY_USDT", "EOSBULL_USDT", "SHIB3L_USDT", "ONE3L_USDT", "SFP_USDT", "GNO_USDT", "EPX_USDT", "YLD_USDT", "TBTC_USDT", "XNL_USDT", "BAS_USDT", "SHOPX_USDT", "DHX_USDT", "REM_USDT", "ARCX_USDT", "EOSDAC_USDT", "LUS_USDT", "ROOM_USDT", "GO_USDT", "DF_USDT", "SNK_USDT", "TPT_USDT", "KWS_USDT", "BEAM3S_USDT", "BCH5L_USDT", "FRIN_USDT", "REVO_USDT", "OMG3L_USDT", "SPELL_USDT", "RFR_USDT", "SD_USDT", "SVT_USDT", "CFI_USDT", "NMR_USDT", "SLM_USDT", "USTC_USDT", "HNS_USDT", "AR3S_USDT", "GALA5S_USDT", "LTC3S_USDT", "MDF_USDT", "TLOS_USDT", "XOR_USDT", "SPHRI_USDT", "CTI_USDT", "MONI_USDT", "SKL3S_USDT", "NANO_USDT", "SDAO_USDT", "PNT_USDT", "VSP_USDT", "SUN_USDT", "SNET_USDT", "DATA_USDT", "KIMCHI_USDT", "ORBR_USDT", "SUKU_USDT", "PIXEL_USDT", "UFI_USDT", "LABS_USDT", "AAVE_USDT", "COVAL_USDT", "TTT_USDT", "MENGO_USDT", "DOGNFT_USDT", "FTI_USDT", "SRM3S_USDT", "ZKS_USDT", "ABBC_USDT", "GSPI_USDT", "BLY_USDT", "JST_USDT", "NCT_USDT", "JULD_USDT", "BAL3L_USDT", "POLIS_USDT", "RANKER_USDT", "AMPL_USDT", "GRIN3S_USDT", "GRBE_USDT", "KINT_USDT", "GLMR3S_USDT", "VISR_USDT", "HOD_USDT", "POSI_USDT", "EDG_USDT", "NEAR3L_USDT", "BABYDOGE_USDT", "NII_USDT", "BCX_USDT", "HID_USDT", "SOP_USDT", "MTL_USDT", "BCHA_USDT", "RAY3L_USDT", "SUNNY_USDT", "BUSY_USDT", "ENJ3L_USDT", "XTZ_USDT", "TRIBE_USDT", "CDT_USDT", "PDEX_USDT", "MDT_USDT", "PRISM_USDT", "DOG_USDT", "FIWA_USDT", "HEART_USDT", "KEX_USDT", "ETH2_USDT", "UNISTAKE_USDT", "JASMY3S_USDT", "BRWL_USDT", "GHC_USDT", "COCOS_USDT", "CUMMIES_USDT", "DOWS_USDT", "ATP_USDT", "ATOM3L_USDT", "JST3S_USDT", "AGLD_USDT", "GOF_USDT", "LEV_USDT", "CHEQ_USDT", "KLO_USDT", "EJS_USDT", "DOGE5L_USDT", "PST_USDT", "AKITA_USDT", "MCRN_USDT", "AURORA_USDT", "ENNO_USDT", "CRV3S_USDT", "DAFI_USDT", "XPRT_USDT", "SAMO_USDT", "PSY_USDT", "MET_USDT", "BERRY_USDT", "RIM_USDT", "TORN_USDT", "HPB_USDT", "KAI_USDT", "DCR_USDT", "1INCH_USDT", "KAVA3S_USDT", "EGG_USDT", "GLMR_USDT", "GAL_USDT", "LEMO_USDT", "OOE_USDT", "LINK3L_USDT", "STRAX_USDT", "PERI_USDT", "POKT_USDT", "AAG_USDT", "PRT_USDT", "YAM_USDT", "DODO_USDT", "ADX_USDT", "ALPHA3L_USDT", "FAN_USDT", "EZ_USDT", "GRIN_USDT", "MXC_USDT", "SONAR_USDT", "SLIM_USDT", "PEOPLE3L_USDT", "COVER_USDT", "DPR_USDT", "BTM3L_USDT", "POLK_USDT", "HOPR_USDT", "SPAY_USDT", "ROCO_USDT", "SPIRIT_USDT", "ULU_USDT", "LARIX_USDT", "INV_USDT", "RUNE3L_USDT", "XRP3L_USDT", "LAYER_USDT", "ATM_USDT", "DAO_USDT", "MOFI_USDT", "MBOX_USDT", "SOL3S_USDT", "CEUR_USDT", "ZLK_USDT", "DEXE_USDT", "MULTI_USDT", "NSURE_USDT", "INK_USDT", "BIFI_USDT", "SUTER_USDT", "HAPI_USDT", "UNI5L_USDT", "BNTY_USDT", "IDV_USDT", "HORD_USDT", "XWG_USDT", "SUPER_USDT", "OCTO_USDT", "MOT_USDT", "KLAY3L_USDT", "GAL3S_USDT", "SOV_USDT", "ALGO3S_USDT", "BXH_USDT", "FLOW_USDT", "RARI_USDT", "SYN_USDT", "DYDX_USDT", "BTRST_USDT", "ATLAS_USDT", "EPIK_USDT", "SLNV2_USDT", "HT_USDT", "XYO_USDT", "AIR_USDT", "BYN_USDT", "GDT_USDT", "SKILL_USDT", "BANK_USDT", "TRX3L_USDT", "ICP_USDT", "GEL_USDT", "REAL_USDT", "PENDLE_USDT", "VERA_USDT", "ARGON_USDT", "GQ_USDT", "YGG_USDT", "THEOS_USDT", "XRP5L_USDT", "STN_USDT", "LTC5S_USDT", "IONX_USDT", "MTL3S_USDT", "BCH3L_USDT", "BZRX_USDT", "AIOZ_USDT", "MODA_USDT", "BEPRO_USDT", "MLN_USDT", "NEO_USDT", "XRD_USDT", "MHUNT_USDT", "MOVR_USDT", "UMA_USDT", "XEC3S_USDT", "ENS_USDT", "WEAR_USDT", "XPR_USDT", "ETC3L_USDT", "TNC_USDT", "BONDED_USDT", "YOP_USDT", "CTC_USDT", "KAR_USDT", "COFI_USDT", "PORT_USDT", "SAITO_USDT", "REVU_USDT", "SGB_USDT", "10SET_USDT", "LUNA3L_USDT", "MOMA_USDT", "STRONG_USDT", "TONCOIN_USDT", "LRC3L_USDT", "ORB_USDT", "PAY_USDT", "GOVI_USDT", "ESS_USDT", "DFL_USDT", "BABY_USDT", "HBAR3L_USDT", "SWAY_USDT", "TOKE_USDT", "CQT_USDT", "RED_USDT", "DAL_USDT", "BRY_USDT", "NGL_USDT", "ACA_USDT", "STPT_USDT", "DXCT_USDT", "ROSE3L_USDT", "PNK_USDT", "ALD_USDT", "FTT3S_USDT", "YFX_USDT", "CSPR3S_USDT", "BIFIF_USDT", "DANA_USDT", "SAND3S_USDT", "RUFF_USDT", "GT_USDT", "ROOBEE_USDT", "INDI_USDT", "OCEAN_USDT", "SIS_USDT", "BSV_USDT", "SRK_USDT", "GHST_USDT", "CELR_USDT", "SAND_USDT", "LINA_USDT", "ILV_USDT", "ADA_USDT", "BNB3S_USDT", "BIRD_USDT", "YOOSHI_USDT", "SUSHI3S_USDT", "MYRA_USDT", "FRONT_USDT", "OHM_USDT", "QTUM3L_USDT", "CRE8_USDT", "UFO_USDT", "UNI3L_USDT", "FUSE_USDT", "SANTOS_USDT", "CRT_USDT", "APX_USDT", "DFYN_USDT", "CULT_USDT", "WGRT_USDT", "AOG_USDT", "FAME_USDT", "ICE_USDT", "1INCH3L_USDT", "DKA_USDT", "FREE_USDT", "XMR3S_USDT", "SCRT_USDT", "YFII3S_USDT", "SNTR_USDT", "SXP3S_USDT", "CRF_USDT", "ALU_USDT", "PUSH_USDT", "QI_USDT", "RAD_USDT", "BSV5S_USDT", "SLRS_USDT", "WXT_USDT", "GF_USDT", "EVRY_USDT", "ANC3S_USDT", "WEMIX_USDT", "APE3S_USDT", "DIO_USDT", "SLP3L_USDT", "PI_USDT", "HBAR_USDT", "XIL_USDT", "ARES_USDT", "GFI_USDT", "AAA_USDT", "SUSHI_USDT", "BDP_USDT", "DEHUB_USDT", "KDA_USDT", "PERC_USDT", "BSV3S_USDT", "XCH3S_USDT", "UDO_USDT", "CAKE3S_USDT", "DASH3S_USDT", "WIN_USDT", "OAX_USDT", "DSD_USDT", "ALPHR_USDT"]
path = "databases/Gate.db"
db = Database(path)
subdata = json.dumps({"id": 12312, "method": "ticker.subscribe", "params": markets})


def write_logs(log):
    logging.info(log)


def push(res, db):
    res = json.loads(res)
    data = res['params'][1]
    print(data)
    df = pd.DataFrame([data])
    df.to_sql(res['params'][0],
              con=db.connection,
              if_exists='append')


def on_message(ws, message):
    push(message, db)


def on_error(ws, error):
    print(error)
    write_logs(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    write_logs(str(close_status_code) + str(close_msg))
    start()


def on_open(ws):
    print("Opened connection")
    ws.send(subdata)


def start():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(uri,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()
