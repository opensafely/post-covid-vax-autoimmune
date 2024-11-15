from ehrql import codelist_from_csv

##################################################################################################################################
# Demographics and Confounders                                                                                                      #
################################################################################################################################## 
# Ethnicity
opensafely_ethnicity_codes_6 = codelist_from_csv(
    "codelists/opensafely-ethnicity.csv",
    column="Code",
    category_column="Grouping_6",
)

primis_covid19_vacc_update_ethnicity = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-eth2001.csv",
    column="code",
    category_column="grouping_6_id",
)
# Smoking
smoking_clear = codelist_from_csv(
    "codelists/opensafely-smoking-clear.csv",
    column="CTV3Code",
    category_column="Category",
)

smoking_unclear = codelist_from_csv(
    "codelists/opensafely-smoking-unclear.csv",
    column="CTV3Code",
    category_column="Category",
)

# AMI
ami_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-ami_snomed.csv",
    column="code",
)
ami_icd10 = codelist_from_csv(
    "codelists/user-RochelleKnight-ami_icd10.csv",
    column="code",
)
ami_prior_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-ami_prior_icd10.csv",
    column="code",
)

# Cancer
cancer_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-cancer_snomed.csv",
    column="code",
)
cancer_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-cancer_icd10.csv",
    column="code",
)

# COPD
copd_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-copd_snomed.csv",
    column="code",
)
copd_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-copd_icd10.csv",
    column="code",
)

stroke_isch_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-stroke_isch_snomed.csv",
    column="code",
)

stroke_isch_icd10 = codelist_from_csv(
    "codelists/user-RochelleKnight-stroke_isch_icd10.csv",
    column="code",
)

# Liver disease
liver_disease_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-liver_disease_snomed.csv",
    column="code",
)
liver_disease_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-liver_disease_icd10.csv",
    column="code",
)

# COCP
cocp_dmd = codelist_from_csv(
    "codelists/user-elsie_horne-cocp_dmd.csv",
    column="code",
)

# Dementia
dementia_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-dementia_snomed.csv",
    column="code",
)
dementia_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-dementia_icd10.csv",
    column="code",
)

# Dementia vascular 
dementia_vascular_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-dementia_vascular_snomed.csv",
    column="code",
)

dementia_vascular_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-dementia_vascular_icd10.csv",
    column="code",
)

# HRT
hrt_dmd = codelist_from_csv(
    "codelists/user-elsie_horne-hrt_dmd.csv",
    column="code",
)

# Hypertension
hypertension_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-hypertension_icd10.csv",
    column="code",
)
hypertension_drugs_dmd = codelist_from_csv(
    "codelists/user-elsie_horne-hypertension_drugs_dmd.csv",
    column="code",
)
hypertension_snomed_clinical = codelist_from_csv(
    "codelists/nhsd-primary-care-domain-refsets-hyp_cod.csv",
    column="code",
)

# Prostate cancer
prostate_cancer_icd10 = codelist_from_csv(
    "codelists/user-RochelleKnight-prostate_cancer_icd10.csv",
    column="code",
)
prostate_cancer_snomed_clinical = codelist_from_csv(
    "codelists/user-RochelleKnight-prostate_cancer_snomed.csv",
    column="code",
)

# Stroke 
stroke_isch_icd10 = codelist_from_csv(
    "codelists/user-RochelleKnight-stroke_isch_icd10.csv",
    column="code",
)
stroke_isch_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-stroke_isch_snomed.csv",
    column="code",
)
# stroke_sah_hs_icd10 = codelist_from_csv(
#     "codelists/user-RochelleKnight-stroke_sah_hs_icd10.csv",
#     column="code",
# )
# stroke_sah_hs_snomed_clinical = codelist_from_csv(
#     "codelists/user-elsie_horne-stroke_sah_hs_snomed.csv",
#     column="code",
# )

# BMI
bmi_obesity_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-bmi_obesity_snomed.csv",
    column="code",
)

bmi_obesity_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-bmi_obesity_icd10.csv",
    column="code",
)

bmi_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-bmi.csv",
    column="code",
)

# Carer codes
carer_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-carer.csv",
    column="code",
)

# No longer a carer codes
notcarer_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-notcarer.csv",
    column="code",
)
# Wider Learning Disability
learndis_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-learndis.csv",
    column="code",
)
# Employed by Care Home codes
carehome_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-carehome.csv",
    column="code",
)

# Employed by nursing home codes
nursehome_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-nursehome.csv",
    column="code",
)

# Employed by domiciliary care provider codes
domcare_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-domcare.csv",
    column="code",
)

# Patients in long-stay nursing and residential care
longres_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-longres.csv",
    column="code",
)


#For JCVI groups
# Pregnancy codes 
preg_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-preg.csv",
    column="code",
)

# Pregnancy or Delivery codes
pregdel_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-pregdel.csv",
    column="code",
)
# All BMI coded terms
bmi_stage_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-bmi_stage.csv",
    column="code",
)
# Severe Obesity code recorded
sev_obesity_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-sev_obesity.csv",
    column="code",
)
# Asthma Diagnosis code
ast_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-ast.csv",
    column="code",
)

# Asthma Admission codes
astadm_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-astadm.csv",
    column="code",
)

# Asthma systemic steroid prescription codes
astrx_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-astrx.csv",
    column="code",
)
# Chronic Respiratory Disease
resp_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-resp_cov.csv",
    column="code",
)
# Chronic Neurological Disease including Significant Learning Disorder
cns_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-cns_cov.csv",
    column="code",
)

# Asplenia or Dysfunction of the Spleen codes
spln_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-spln_cov.csv",
    column="code",
)
# Diabetes diagnosis codes
diab_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-diab.csv",
    column="code",
)
# Diabetes resolved codes
dmres_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-dmres.csv",
    column="code",
)
# Severe Mental Illness codes
sev_mental_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-sev_mental.csv",
    column="code",
)

# Remission codes relating to Severe Mental Illness
smhres_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-smhres.csv",
    column="code",
)

# Chronic heart disease codes
chd_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-chd_cov.csv",
    column="code",
)

# Chronic Kidney disease
ckd_snomed_clinical = codelist_from_csv(
    "codelists/user-elsie_horne-ckd_snomed.csv",
    column="code",
)

ckd_icd10 = codelist_from_csv(
    "codelists/user-elsie_horne-ckd_icd10.csv",
    column="code",
)

# Chronic kidney disease diagnostic codes
ckd_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-ckd_cov.csv",
    column="code",
)

# Chronic kidney disease codes - all stages
ckd15_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-ckd15.csv",
    column="code",
)

# Chronic kidney disease codes-stages 3 - 5
ckd35_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-ckd35.csv",
    column="code",
)

# Chronic Liver disease codes
cld_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-cld.csv",
    column="code",
)
# Immunosuppression diagnosis codes
immdx_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-immdx_cov.csv",
    column="code",
)

# Immunosuppression medication codes
immrx_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-immrx.csv",
    column="code",
)

# Diabetes
# Type 1 diabetes
diabetes_type1_snomed_clinical = codelist_from_csv(
    "codelists/user-hjforbes-type-1-diabetes.csv",
    column="code",
)
# Type 1 diabetes secondary care
diabetes_type1_icd10 = codelist_from_csv(
    "codelists/opensafely-type-1-diabetes-secondary-care.csv",
    column="icd10_code",
)
# Type 2 diabetes
diabetes_type2_snomed_clinical = codelist_from_csv(
    "codelists/user-hjforbes-type-2-diabetes.csv",
    column="code",
)
# Type 2 diabetes secondary care
diabetes_type2_icd10 = codelist_from_csv(
    "codelists/user-r_denholm-type-2-diabetes-secondary-care-bristol.csv",
    column="code",
)
# Non-diagnostic diabetes codes
diabetes_diagnostic_snomed_clinical = codelist_from_csv(
    "codelists/user-hjforbes-nondiagnostic-diabetes-codes.csv",
    column="code",
)
# Other or non-specific diabetes
diabetes_other_snomed_clinical = codelist_from_csv(
    "codelists/user-hjforbes-other-or-nonspecific-diabetes.csv",
    column="code",
)
# Gestational diabetes
diabetes_gestational_snomed_clinical = codelist_from_csv(
    "codelists/user-hjforbes-gestational-diabetes.csv",
    column="code",
)
# Insulin medication 
insulin_snomed_clinical = codelist_from_csv(
     "codelists/opensafely-insulin-medication.csv",
     column="code",
)
# Antidiabetic drugs
antidiabetic_drugs_snomed_clinical = codelist_from_csv(
     "codelists/opensafely-antidiabetic-drugs.csv",
     column="code",
)
# Antidiabetic drugs - non metformin
non_metformin_dmd = codelist_from_csv(
    "codelists/user-r_denholm-non-metformin-antidiabetic-drugs_bristol.csv", 
    column="code",
)
# Prediabetes
prediabetes_snomed = codelist_from_csv(
    "codelists/opensafely-prediabetes-snomed.csv",
    column="code",
)

## Quality assurance codes 

prostate_cancer_snomed_clinical = codelist_from_csv(
    "codelists/user-RochelleKnight-prostate_cancer_snomed.csv",
    column="code",
)
prostate_cancer_icd10 = codelist_from_csv(
    "codelists/user-RochelleKnight-prostate_cancer_icd10.csv",
    column="code",
)
pregnancy_snomed_clinical = codelist_from_csv(
    "codelists/user-RochelleKnight-pregnancy_and_birth_snomed.csv",
    column="code",
)

# # Dementia
# dementia_snomed_clinical = codelist_from_csv(
#     "codelists/user-elsie_horne-dementia_snomed.csv",
#     column="code",
# )
# dementia_icd10 = codelist_from_csv(
#     "codelists/user-elsie_horne-dementia_icd10.csv",
#     column="code",
# )

##################################################################################################################################
# COVID-19 risk, disease and vaccination codes                                                                                                       #
################################################################################################################################## 

###############################################################################
# At risk from COVID-19                                                       #
###############################################################################
# High Risk from COVID-19 code
shield_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-shield.csv",
    column="code",
)

# Lower Risk from COVID-19 codes
nonshield_primis = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-nonshield.csv",
    column="code",
)

###############################################################################
# COVID-19 disease codes                                                      #
###############################################################################
# COVID-19 infection
covid_primary_care_positive_test = codelist_from_csv(
    "codelists/opensafely-covid-identification-in-primary-care-probable-covid-positive-test.csv",
    column="CTV3ID",
)

covid_primary_care_code = codelist_from_csv(
    "codelists/opensafely-covid-identification-in-primary-care-probable-covid-clinical-code.csv",
    column="CTV3ID",
)

covid_primary_care_sequalae = codelist_from_csv(
    "codelists/opensafely-covid-identification-in-primary-care-probable-covid-sequelae.csv",
    column="CTV3ID",
)

covid_codes = codelist_from_csv(
    "codelists/user-RochelleKnight-confirmed-hospitalised-covid-19.csv",
    column="code",
)

#############################################################################
# COVID-19 vaccination                                                      #
#############################################################################
## COVID-19 vaccination
# 1st dose
vac_adm_1 = codelist_from_csv(
  "codelists/primis-covid19-vacc-uptake-covadm1.csv",
  column="code"
)

# 2nd dose
vac_adm_2 = codelist_from_csv(
  "codelists/primis-covid19-vacc-uptake-covadm2.csv",
  column="code"
)

# 3rd dose
vac_adm_3 = codelist_from_csv(
  "codelists/primis-covid19-vacc-uptake-covadm3_cod.csv",
  column="code"
)

# 4th dose
vac_adm_4 = codelist_from_csv(
  "codelists/primis-covid19-vacc-uptake-covadm4_cod.csv",
  column="code"
)

# 5th dose
vac_adm_5 = codelist_from_csv(
  "codelists/primis-covid19-vacc-uptake-covadm5_cod.csv",
  column="code"
)

# oxford-astrazeneca dose
vac_chadox1_dose = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-azdrx.csv",
    column = "code"
)

# moderna dose
vac_mrna_dose = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-modrx.csv",
    column = "code"
)

# pfizer dose
vac_bnt_dose = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-pfdrx.csv",
    column = "code"
)


##################################################################################################################################
# Autoimmune disease codes                                                                                                       #
################################################################################################################################## 

##################################################################################################
# Outcome group 1:  Inflammatory arthritis                                                       #
##################################################################################################
# Reumatoid arthritis - snomed
ra_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-polymyalgia-rheumatica.csv",
    column="code",
)
# Reumatoid arthritis - icd
ra_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-arthritis-rheumatoid-arthritis-icd10.csv",
    column="code",
)
# Undifferentiated inflamatory arthritis - snomed
undiff_eia_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-undiff-eia.csv",
    column="code",
)
# undifferentiated inflmatory arthritis - no icd10 codelist for this disease, 14 Dec 2022 YW

# Psoriatic arthritis - snomed
psoa_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-psoriatic-arthritis.csv",
    column = "code",
)
# Psoriatic arthritis - icd10 
psoa_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-arthritis-psoriatic-arthritis-icd10.csv",
    column = "code",
)
# Axial spondyloarthritis - snomed
axial_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-axial-spondyloarthritis.csv",
    column = "code",
)
# Axial spondyloarthritis - hes
axial_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-arthritis-axial-spondyloarthritis-icd10.csv",
    column = "code",
)
##################################################################################################
# Outcome group 2:  Connective tissue disorders                                                  #
##################################################################################################
# Systematic lupus erythematosu - ctv
sle_code_ctv = codelist_from_csv(
    "codelists/opensafely-systemic-lupus-erythematosus-sle.csv",#user-markdrussell-systemic-sclerosisscleroderma.csv",
    column="CTV3ID",
)
# Systematic lupus erythematosu - hes
sle_code_icd = codelist_from_csv(
    "codelists/user-josephignace-connective-tissue-disorders-systematic-lupus-erythematosus.csv",
    column="code",
)
#Sjogren’s syndrome - snomed
sjs_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-sjogrens-syndrome.csv",
    column="code",
)
#Sjogren’s syndrome - hes
sjs_code_icd = codelist_from_csv(
    "codelists/user-josephignace-connective-tissue-disorders-sjogrens-syndrome.csv",
    column="code",
)
#Systemic sclerosis/scleroderma - snomed
sss_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-systemic-sclerosisscleroderma.csv",
    column="code",
)
#Systemic sclerosis/scleroderma - hes
sss_code_icd = codelist_from_csv(
    "codelists/user-josephignace-connective-tissue-disorders-systemic-sclerosisscleroderma.csv",
    column="code",
)
# Inflammatory myositis/polymyositis/dermatolomyositis - snomed
im_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-inflammatory-myositis.csv",
    column="code",
)
# Inflammatory myositis/polymyositis/dermatolomyositis - hes
im_code_icd = codelist_from_csv(
    "codelists/user-josephignace-connective-tissue-disorders-inflammatory-myositispolymyositisdermatolomyositis.csv",
    column="code",
)
# Mixed Connective Tissue Disease - snomed
mctd_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-mctd.csv",
    column="code",
)
# Mixed Connective Tissue Disease - hes
mctd_code_icd = codelist_from_csv(
    "codelists/user-josephignace-connective-tissue-disorders-mixed-connective-tissue-disease.csv",
    column="code",
)
# Antiphospholipid syndrome - snomed
as_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-antiphospholipid-syndrome.csv",
    column="code",
)
# Antiphospholipid syndrome - no icd10 code
##################################################################################################
# Outcome group 3: Inflammatory skin disease                                                     #
##################################################################################################
## Psoriasis - primary care - ctv3
psoriasis_code_ctv = codelist_from_csv(
    "codelists/opensafely-psoriasis.csv",
    column="code",
)
## Psoriasis - secondary care - icd10
psoriasis_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-skin-disease-psoriasis.csv",
    column="code",
)
## Hydradenitis suppurativa - ctv3
hs_code_ctv = codelist_from_csv(
    "codelists/opensafely-hidradenitis-suppurativa.csv",
    column="CTV3ID",
)
## Hydradenitis suppurativa - hes
hs_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-skin-disease-hidradenitis-suppurativa-icd10.csv",
    column="code",
)
##################################################################################################
# Outcome group 4: Autoimmune GI/Inflammatory bowel disease                                      #
##################################################################################################
# Inflammatory bowel disease (combined UC and Crohn's) - snomed
ibd_code_snomed = codelist_from_csv(
    "codelists/opensafely-inflammatory-bowel-disease-snomed.csv",
    column="id",
)
# Inflammatory bowel disease (combined UC and Crohn's) - ctv3
ibd_code_ctv3 = codelist_from_csv(
    "codelists/opensafely-inflammatory-bowel-disease.csv",
    column="CTV3ID",
)
# YW notes 17 Jan 2023: the ICD10 codelist for IBD doesn't work: 
# https://www.opencodelists.org/codelist/user/josephignace/autoimmune-gi-inflammatory-bowel-disease-inflammatory-bowel-disease-combined-uc-and-crohns/7224b1b6/
ibd_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-gi-inflammatory-bowel-disease-inflammatory-bowel-disease-combined-uc-and-crohns.csv",
    column = "code",
)

# Crohn's disease - ctv
crohn_code_ctv = codelist_from_csv(
    "codelists/opensafely-crohns-disease.csv",
    column="code",
)
# Crohn's disease - hes
crohn_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-gi-inflammatory-bowel-disease-crohns-disease-icd10.csv",
    column="code",
)
# Ulcerative colitis - ctv
uc_code_ctv = codelist_from_csv(
    "codelists/opensafely-ulcerative-colitis.csv",
    column="code",
)
# Ulcerative colitis - hes
uc_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-gi-inflammatory-bowel-disease-ulcerative-colitis-icd10.csv",
    column="code",
)
# Celiac disease - snomed
celiac_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-celiaccoeliac-disease.csv",
    column="code",
)
# Celiac disease - hes
celiac_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-gi-inflammatory-bowel-disease-celiac-disease-icd10.csv",
    column="code",
)
##################################################################################################
# Outcome group 5: Thyroid diseases                                                              #
##################################################################################################
# Addison’s disease - snomed
addison_code_snomed = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-addis_cod.csv",
    column="code",
)
# Addison’s disease - hes
addison_code_icd= codelist_from_csv(
    "codelists/user-josephignace-thyroid-diseases-addisons-disease-icd10.csv",
    column="code",
)
# Grave’s disease - snomed
grave_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-graves-disease.csv",
    column="code",
)
# Grave’s disease - hes
grave_code_icd = codelist_from_csv(
    "codelists/user-josephignace-thyroid-diseases-graves-disease-icd10.csv",
    column="code",
)
# Hashimoto’s thyroiditis - snomed
hashimoto_thyroiditis_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-hashimotos-thyroiditis-autoimmune-thyroiditis.csv",
    column="code",
)
# Hashimoto’s thyroiditis - hes
hashimoto_thyroiditis_code_icd = codelist_from_csv(
    "codelists/user-josephignace-thyroid-diseases-hashimoto-thyroiditis-icd10.csv",
    column="code",
)
# Thyroid toxicosis / hyper thyroid - YW: This seems to have been taken out from the excel spreadsheet 13/Dec/2022
##################################################################################################
# Outcome group 6: Autoimmune vasculitis                                                          #
##################################################################################################
# ANCA-associated - snomed
anca_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-anca-vasculitis.csv",
    column="code",
)
# ANCA-associated - hes
anca_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-vasculitis-anca-associated.csv",
    column="code",
)
# Giant cell arteritis - snomed
gca_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-anca-vasculitis.csv",
    column="code",
)
# Giant cell arteritis - hes
gca_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-vasculitis-giant-cell-arteritis.csv",
    column="code",
)
# IgA (immunoglobulin A) vasculitis - snomed
iga_vasculitis_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-iga-immunoglobulin-a-vasculitis.csv",
    column="code",
)
# IgA (immunoglobulin A) vasculitis - hes
iga_vasculitis_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-vasculitis-iga-immunoglobulin-a-vasculitis.csv",
    column="code",
)
# Polymyalgia Rheumatica (PMR) - snomed
pmr_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-polymyalgia-rheumatica.csv",
    column="code",
)
# Polymyalgia Rheumatica (PMR) - hes
pmr_code_icd = codelist_from_csv(
    "codelists/user-josephignace-autoimmune-vasculitis-polymyalgia-rheumatica-pmr.csv",
    column="code",
)
##################################################################################################
# Outcome group 7: Hematologic Diseases                                                          #
##################################################################################################
# Immune thrombocytopenia (formerly known as idiopathic thrombocytopenic purpura) - snomed
immune_thromb_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-immune-thrombocytopenia-idiopathic-thrombocytopenic-purpura.csv",
    column="code",
)
# Immune thrombocytopenia (formerly known as idiopathic thrombocytopenic purpura) - hes
immune_thromb_code_icd = codelist_from_csv(
    "codelists/user-josephignace-hematologic-diseases-immune-thrombocytopenia-formerly-known-as-idiopathic-thrombocytopenic-purpura-icd10.csv",
    column="code",
)
# Pernicious anaemia - snomed
pernicious_anaemia_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-pernicious-anaemia.csv",
    column="code",
)
# Pernicious anaemia - hes
pernicious_anaemia_code_icd = codelist_from_csv(
    "codelists/user-josephignace-hematologic-diseases-pernicious-anaemia-icd10.csv",
    column="code",
)
# Aplastic Anaemia - snomed
apa_code_snomed = codelist_from_csv(
    "codelists/opensafely-aplastic-anaemia-snomed.csv",
    column="id",
)
# Aplastic Anaemia - ctv3
apa_code_ctv = codelist_from_csv(
    "codelists/opensafely-aplastic-anaemia.csv",
    column="CTV3ID",
)
# Aplastic Anaemia - hes
apa_code_icd = codelist_from_csv(
    "codelists/user-josephignace-hematologic-diseases-aplastic-anaemia-icd10.csv",
    column="code",
)
# Autoimmune haemolytic anaemia - snomed
aha_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-autoimmune-haemolytic-anaemia-autoimmune-hemolytic-anemia.csv",
    column="code",
)
# Autoimmune haemolytic anaemia - hes
aha_code_icd = codelist_from_csv(
    "codelists/user-josephignace-hematologic-diseases-autoimmune-haemolytic-anaemia-icd10.csv",
    column="code",
)
##################################################################################################
# Outcome group 8: Inflammatory neuromuscular disease                                            #
##################################################################################################
# Guillain Barré - read
glb_code_ctv = codelist_from_csv(
    "codelists/opensafely-guillain-barre.csv",
    column="code",
)
# Guillain Barré - hes
glb_code_icd = codelist_from_csv(
    "codelists/opensafely-guillain-barre-syndrome-icd10.csv",
    column="code",
)
# Multiple Sclerosis - read
multiple_sclerosis_code_ctv = codelist_from_csv(
    "codelists/opensafely-multiple-sclerosis-v2.csv",
    column="code",
)
# Multiple Sclerosis - hes
multiple_sclerosis_code_icd = codelist_from_csv(
    "codelists/user-yinghuiwei-multiple-sclerosis.csv",
    column="code",
)
# Myasthenia gravis - snomed
myasthenia_gravis_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-myasthenia-gravis.csv",
    column="code",
)
# Myasthenia gravis - hes
myasthenia_gravis_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-neuromuscular-disease-myasthenia-gravis-icd10.csv",
    column="code",
)
# Longitudinal myelitis - snomed
longit_myelitis_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-longitudinal-myelitis-longitudinal-extensive-transverse-myelitis.csv",
    column="code",
)
# Longitudinal myelitis - hes
longit_myelitis_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-neuromuscular-disease-longitudinal-myelitis-icd10.csv",
    column="code",
)
# Clinically isolated syndrome - snomed
cis_code_snomed = codelist_from_csv(
    "codelists/user-markdrussell-clinically-isolated-syndrome.csv",
    column="code",
)
# Clinically isolated syndrome - hes
cis_code_icd = codelist_from_csv(
    "codelists/user-josephignace-inflammatory-neuromuscular-disease-clinically-isolated-syndrome-icd10.csv",
    column="code",
)