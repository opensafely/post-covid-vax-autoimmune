from ehrql import create_dataset, case, when, days, minimum_of
from ehrql.tables.tpp import (
    patients, 
    practice_registrations, 
    addresses, 
    clinical_events, 
    appointments,
    occupation_on_covid_vaccine_record,
    apcs,
    opa_diag,
    medications,
    ons_deaths,
    vaccinations
)

# import codelists
from codelists import *

# create dataset
dataset = create_dataset()

# set size of dummy dataset
dataset.configure_dummy_data(population_size = 1000, timeout = 600)

# define start and end dates
start_date = "2020-01-29"
end_date = "2022-03-31"

# prior follow-up start and end dates
prior_followup_start_date = "2019-07-29"
prior_followup_end_date = "2021-09-30"

## defining inclusion criteria
# find patients who are have 6 month prior follow-up in study period
has_registration = practice_registrations.spanning(prior_followup_start_date, start_date)

# find patients with known deprivation
has_deprivation_index = addresses.for_patient_on(
    start_date
).imd_rounded.is_not_null()

# find patients with known region
has_region = practice_registrations.for_patient_on(
    start_date
).practice_nuts1_region_name.is_not_null()

# find patients who were alive on study start date
is_alive = (patients.is_alive_on(start_date) & 
             ((ons_deaths.date.is_null()) | 
              (ons_deaths.date.is_after(start_date))))


## create dataset with inclusion criteria
dataset.define_population(
    (patients.date_of_birth.is_on_or_before("2002-03-31")) & 
    (patients.date_of_birth.is_on_or_after("1910-03-31")) & 
    (has_registration.exists_for_patient()) &
    ((patients.sex == "male") | (patients.sex == "female")) &
    (has_deprivation_index) &
    (has_region) &
    (is_alive))


## add potential confounders to dataset
# add registration and deregistration dates
dataset.date_registered = practice_registrations.sort_by(practice_registrations.start_date).last_for_patient().start_date
dataset.date_deregistered = practice_registrations.sort_by(practice_registrations.end_date).last_for_patient().end_date

# add patients date of birth as column
dataset.dob = patients.date_of_birth

# add date of death column
dataset.death_date = ons_deaths.date

# add patient's sex as column
dataset.sex = patients.sex

# add ethnicity column
dataset.ethnicity = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(opensafely_ethnicity_codes_6)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(opensafely_ethnicity_codes_6)
)

# add imd decile column
imd_decile = addresses.for_patient_on(start_date).imd_decile
dataset.imd_decile = imd_decile

# add region column
region = (practice_registrations.for_patient_on(start_date)
          .practice_nuts1_region_name)

dataset.region = region

# add consultation rate column
dataset.consultation_rate = appointments.where(
    appointments.status.is_in([
        "Arrived",
        "In Progress",
        "Finished",
        "Visit",
        "Waiting",
        "Patient Walked Out",
    ]) & appointments.start_date.is_on_or_between(start_date - days(365), start_date)
).count_for_patient()

# add smoking status column
dataset.smoking_status = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(smoking_clear)
    )
    .where(clinical_events.date.is_before(start_date))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(smoking_clear)
)

# add healthcare worker column
dataset.healthcare_worker = occupation_on_covid_vaccine_record.where(
    (occupation_on_covid_vaccine_record.is_healthcare_worker == True)
).exists_for_patient()

# add carehome resident column
dataset.carehome_resident = (    
    addresses.for_patient_on(start_date).care_home_is_potential_match |
    addresses.for_patient_on(start_date).care_home_requires_nursing |
    addresses.for_patient_on(start_date).care_home_does_not_require_nursing
)


## covariates from clinical events and apcs tables
# defining "before" tables
clinical_events_before = clinical_events.where(clinical_events.date.is_before(start_date))
apcs_before = apcs.where(apcs.admission_date.is_before(start_date))
opa_diag_before = opa_diag.where(opa_diag.appointment_date.is_before(start_date))

# creating function for "clinical event" covariates
def diagnosis_of(column_name, snomedcode, icd_code):

  dataset.add_column(column_name, (    
      (clinical_events.where(
          (clinical_events.snomedct_code.is_in(snomedcode)) &
      (clinical_events.date.is_before(start_date))
      ).exists_for_patient()) |
      (apcs.where(
          ((apcs.primary_diagnosis.is_in(icd_code)) | 
          (apcs.secondary_diagnosis.is_in(icd_code))) &
          (apcs.admission_date.is_before(start_date))
      ).exists_for_patient()) |
      (opa_diag.where(
          ((opa_diag.primary_diagnosis_code.is_in(icd_code)) | 
          (opa_diag.secondary_diagnosis_code_1.is_in(icd_code))) &
          (opa_diag.appointment_date.is_before(start_date))
      ).exists_for_patient())
  )
  )

# add dementia column
diagnosis_of("dementia", dementia_snomed_clinical + dementia_vascular_snomed_clinical,
             dementia_icd10 + dementia_vascular_icd10)

# add liver disease column
diagnosis_of("liver_disease", liver_disease_snomed_clinical, liver_disease_icd10)

# add chonic kidney disease column
diagnosis_of("chronic_kidney_disease", ckd_snomed_clinical, ckd_icd10)

# add cancer column
diagnosis_of("cancer", cancer_snomed_clinical, cancer_icd10)

# add hypertention column
dataset.hypertension = (
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(hypertension_snomed_clinical)) &
        (clinical_events.date.is_before(start_date))
    ).exists_for_patient()) |
    (medications.where(
        ((medications.dmd_code.is_in(hypertension_drugs_dmd))) &
        (medications.date.is_before(start_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(hypertension_icd10)) |
        (apcs.secondary_diagnosis.is_in(hypertension_icd10))) &
        (apcs.admission_date.is_before(start_date))
    ).exists_for_patient()) |
            (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(hypertension_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(hypertension_icd10))) &
        (opa_diag.appointment_date.is_before(start_date))
    ).exists_for_patient())
)

# add diabetes column
dataset.diabetes = (
    (clinical_events.where(
        (clinical_events.ctv3_code.is_in(diabetes_type1_snomed_clinical + 
        diabetes_type2_snomed_clinical + 
        diabetes_diagnostic_snomed_clinical + 
        diabetes_other_snomed_clinical + 
        diabetes_gestational_snomed_clinical)) &
        (clinical_events.date.is_before(start_date))
    ).exists_for_patient()) |
    (medications.where(
        ((medications.dmd_code.is_in(insulin_snomed_clinical)) | 
        (medications.dmd_code.is_in(antidiabetic_drugs_snomed_clinical)) |
        (medications.dmd_code.is_in(non_metformin_dmd))) &
        (medications.date.is_before(start_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(diabetes_type1_icd10)) | 
        (apcs.primary_diagnosis.is_in(diabetes_type2_icd10)) |
        (apcs.secondary_diagnosis.is_in(diabetes_type1_icd10)) |
        (apcs.secondary_diagnosis.is_in(diabetes_type2_icd10))) &
        (apcs.admission_date.is_before(start_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(diabetes_type1_icd10)) | 
        (opa_diag.primary_diagnosis_code.is_in(diabetes_type2_icd10)) |
        (opa_diag.secondary_diagnosis_code_1.is_in(diabetes_type1_icd10)) |
        (opa_diag.secondary_diagnosis_code_1.is_in(diabetes_type2_icd10))) &
        (opa_diag.appointment_date.is_before(start_date))
    ).exists_for_patient())
)

# add obesity column
diagnosis_of("obesity", bmi_obesity_snomed_clinical, bmi_obesity_icd10)

# add chronic obstructive pulmonary diease (copd) column
diagnosis_of("copd", copd_snomed_clinical, copd_icd10)

# add acute myocardial infarction (ami) column
diagnosis_of("ami", ami_snomed_clinical, ami_icd10 + ami_prior_icd10)

# add ischaemic stroke column
diagnosis_of("ischaemic_stroke", stroke_isch_snomed_clinical, stroke_isch_icd10)


## add exposures to dataset
# add date of COVID19 column
covid19_primarycare_date = (clinical_events.where(
    (clinical_events.ctv3_code.is_in(covid_primary_care_positive_test +
                                    covid_primary_care_code +
                                    covid_primary_care_sequalae))
).sort_by(clinical_events.date).first_for_patient().date)

covid19_secondarycare_date1 = (apcs.where(
    ((apcs.primary_diagnosis.is_in(covid_codes)) |
    (apcs.secondary_diagnosis.is_in(covid_codes)))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

covid19_secondarycare_date2 = (opa_diag.where(
    ((opa_diag.primary_diagnosis_code.is_in(covid_codes)) |
    (opa_diag.secondary_diagnosis_code_1.is_in(covid_codes)))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.covid19_date = minimum_of(covid19_primarycare_date,
                                  covid19_secondarycare_date1,
                                  covid19_secondarycare_date2)

# add date of COVID19 vaccine columns
covid_vaccinations = (
  vaccinations
  .where(vaccinations.target_disease.is_in(["SARS-2 CORONAVIRUS"]))
  .sort_by(vaccinations.date)
)

previous_vax_date = "1899-01-01"

for i in range(1, 10+1):

    current_vax = covid_vaccinations.where(covid_vaccinations.date>previous_vax_date).first_for_patient()
    dataset.add_column(f"covid_vax_{i}_date", current_vax.date)
    dataset.add_column(f"covid_vax_type_{i}", current_vax.product_name)
    previous_vax_date = current_vax.date


## add outcome dates to the dataset
# add date of rheumatoid arthritis onset column
rheumatoid_arthritis_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(ra_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

rheumatoid_arthritis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(ra_code_icd) |
    apcs.secondary_diagnosis.is_in(ra_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

rheumatoid_arthritis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(ra_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(ra_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.rheumatoid_arthritis = minimum_of(rheumatoid_arthritis_primary_date,
                                              rheumatoid_arthritis_secondary_date1,
                                              rheumatoid_arthritis_secondary_date2)

# add date of undifferentiated inflammatory arthritis (undiff_eia) onset column
dataset.undiff_eia = (clinical_events.where(
    clinical_events.snomedct_code.is_in(undiff_eia_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

# add date of psoriatic arthritis onset column
psoriatic_arthritis_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(psoa_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

psoriatic_arthritis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(psoa_code_icd) |
    apcs.secondary_diagnosis.is_in(psoa_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

psoriatic_arthritis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(psoa_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(psoa_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.psoriatic_arthitis = minimum_of(psoriatic_arthritis_primary_date,
                                        psoriatic_arthritis_secondary_date1,
                                        psoriatic_arthritis_secondary_date2)

# add date of axial spondyloarthritis (axial) onset column
axial_arthritis_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(axial_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

axial_arthritis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(axial_code_icd) |
    apcs.secondary_diagnosis.is_in(axial_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

axial_arthritis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(axial_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(axial_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.axial = minimum_of(axial_arthritis_primary_date,
                           axial_arthritis_secondary_date1,
                           axial_arthritis_secondary_date2)

# add date of any group 1 AI outcome onset column
dataset.grp1_outcome = minimum_of(dataset.rheumatoid_arthritis,
                                  dataset.undiff_eia,
                                  dataset.psoriatic_arthitis,
                                  dataset.axial)

# add date of systemic lupus erythematosus (sle) onset column
sle_primary_date = (clinical_events.where(
    clinical_events.ctv3_code.is_in(sle_code_ctv)
).sort_by(clinical_events.date).first_for_patient().date)

sle_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(sle_code_icd) |
    apcs.secondary_diagnosis.is_in(sle_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

sle_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(sle_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(sle_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.sle = minimum_of(sle_primary_date,
                         sle_secondary_date1,
                         sle_secondary_date2)

# add date of sjogren's syndrome onset column
sjogren_syndrome_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(sjs_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

sjogren_syndrome_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(sjs_code_icd) |
    apcs.secondary_diagnosis.is_in(sjs_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

sjogren_syndrome_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(sjs_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(sjs_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.sjogren_syndrome = minimum_of(sjogren_syndrome_primary_date,
                                      sjogren_syndrome_secondary_date1,
                                      sjogren_syndrome_secondary_date2)

# add date of systemic sclerosis/scleroderma (sys_sclerosis) onset column
sss_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(sss_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

sss_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(sss_code_icd) |
    apcs.secondary_diagnosis.is_in(sss_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

sss_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(sss_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(sss_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.sys_sclerosis = minimum_of(sss_primary_date,
                                   sss_secondary_date1,
                                   sss_secondary_date2)

# add date of inflammatory myositis/polymyositis/dermatolomyositis (infl_myositis) onset column
im_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(im_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

im_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(im_code_icd) |
    apcs.secondary_diagnosis.is_in(im_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

im_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(im_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(im_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.infl_myositis = minimum_of(im_primary_date,
                                   im_secondary_date1,
                                   im_secondary_date2)

# add date of mixed connective tissue disease (mctd) onset column
mctd_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(mctd_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

mctd_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(mctd_code_icd) |
    apcs.secondary_diagnosis.is_in(mctd_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

mctd_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(mctd_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(mctd_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.mctd = minimum_of(mctd_primary_date,
                          mctd_secondary_date1,
                          mctd_secondary_date2)

# add date of antiphospholipid syndrome (antiphos_syndrome) onset column
dataset.antiphos_syndrome = (clinical_events.where(
    clinical_events.snomedct_code.is_in(as_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

# add date of any group2 AI outcome onset column
dataset.grp2_outcome = minimum_of(dataset.sle,
                                  dataset.sjogren_syndrome,
                                  dataset.sys_sclerosis,
                                  dataset.infl_myositis,
                                  dataset.mctd,
                                  dataset.antiphos_syndrome)

# add date of psoriasis onset column
psoriasis_primary_date = (clinical_events.where(
    clinical_events.ctv3_code.is_in(psoriasis_code_ctv)
).sort_by(clinical_events.date).first_for_patient().date)

psoriasis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(psoriasis_code_icd) |
    apcs.secondary_diagnosis.is_in(psoriasis_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

psoriasis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(psoriasis_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(psoriasis_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.psoriasis = minimum_of(psoriasis_primary_date,
                               psoriasis_secondary_date1,
                               psoriasis_secondary_date2)

# add date of hydradenitis suppurativa (hyrda_supp) onset column
hs_primary_date = (clinical_events.where(
    clinical_events.ctv3_code.is_in(hs_code_ctv)
).sort_by(clinical_events.date).first_for_patient().date)

hs_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(hs_code_icd) |
    apcs.secondary_diagnosis.is_in(hs_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

hs_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(hs_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(hs_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.hydra_supp = minimum_of(hs_primary_date,
                                hs_secondary_date1,
                                hs_secondary_date2)

# add date of any group3 AI outcome onset column
dataset.grp3_outcome = minimum_of(dataset.psoriasis,
                                  dataset.hydra_supp)

# add date of crohn's diseases onset column
cd_primary_date = (clinical_events.where(
    clinical_events.ctv3_code.is_in(crohn_code_ctv)
).sort_by(clinical_events.date).first_for_patient().date)

cd_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(crohn_code_icd) |
    apcs.secondary_diagnosis.is_in(crohn_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

cd_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(crohn_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(crohn_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.crohn_disease = minimum_of(hs_primary_date,
                                   hs_secondary_date1,
                                   hs_secondary_date2)

# add date of ulcerative colitis onset column
uc_primary_date = (clinical_events.where(
    clinical_events.ctv3_code.is_in(uc_code_ctv)
).sort_by(clinical_events.date).first_for_patient().date)

uc_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(uc_code_icd) |
    apcs.secondary_diagnosis.is_in(uc_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

uc_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(uc_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(uc_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.ulcerative_colitis = minimum_of(uc_primary_date,
                                        uc_secondary_date1,
                                        uc_secondary_date2)

# add date of celiac disease onset column
celiac_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(celiac_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

celiac_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(celiac_code_icd) |
    apcs.secondary_diagnosis.is_in(celiac_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

celiac_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(celiac_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(celiac_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.celiac_disease = minimum_of(celiac_primary_date,
                                    celiac_secondary_date1,
                                    celiac_secondary_date2)

# add date of inflammatory bowel disease (ibd) onset column
ibd_primary_date = (clinical_events.where(
    (clinical_events.snomedct_code.is_in(ibd_code_snomed) |
    clinical_events.ctv3_code.is_in(ibd_code_ctv3))
).sort_by(clinical_events.date).first_for_patient().date)

ibd_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(ibd_code_icd) |
    apcs.secondary_diagnosis.is_in(ibd_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

ibd_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(ibd_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(ibd_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.ibd = minimum_of(ibd_primary_date,
                         ibd_secondary_date1,
                         ibd_secondary_date2)

# add date of any group4 AI outcome onset column
dataset.grp4_outcome = minimum_of(dataset.crohn_disease,
                                  dataset.ulcerative_colitis,
                                  dataset.celiac_disease,
                                  dataset.ibd)

# add date of addison's disease onset column
addison_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(addison_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

addison_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(addison_code_icd) |
    apcs.secondary_diagnosis.is_in(addison_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

addison_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(addison_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(addison_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.addison_disease = minimum_of(addison_primary_date,
                                     addison_secondary_date1,
                                     addison_secondary_date2)

# add date of grave's disease onset column
grave_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(grave_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

grave_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(grave_code_icd) |
    apcs.secondary_diagnosis.is_in(grave_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

grave_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(grave_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(grave_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.grave_disease = minimum_of(grave_primary_date,
                                   grave_secondary_date1,
                                   grave_secondary_date2)

# add date of hashimoto's thyroiditis onset column
hashimoto_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(hashimoto_thyroiditis_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

hashimoto_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(hashimoto_thyroiditis_code_icd) |
    apcs.secondary_diagnosis.is_in(hashimoto_thyroiditis_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

hashimoto_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(hashimoto_thyroiditis_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(hashimoto_thyroiditis_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.hashimoto_thyroiditis = minimum_of(hashimoto_primary_date,
                                           hashimoto_secondary_date1,
                                           hashimoto_secondary_date2)

# add date of any group5 AI outcome onset column
dataset.grp5_outcome = minimum_of(dataset.addison_disease,
                                  dataset.grave_disease,
                                  dataset.hashimoto_thyroiditis)

# add date of anca associated onset column
anca_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(anca_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

anca_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(anca_code_icd) |
    apcs.secondary_diagnosis.is_in(anca_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

anca_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(anca_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(anca_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.anca_associated = minimum_of(anca_primary_date,
                                     anca_secondary_date1,
                                     anca_secondary_date2)

# add date of giant cell arteritis onset column
gca_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(gca_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

gca_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(gca_code_icd) |
    apcs.secondary_diagnosis.is_in(gca_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

gca_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(gca_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(gca_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.giant_cell_arteritis = minimum_of(gca_primary_date,
                                          gca_secondary_date1,
                                          gca_secondary_date2)

# add date of immunoglobulin A vascultisit (iga_vasculitis) onset column
iga_vasculitis_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(iga_vasculitis_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

iga_vasculitis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(iga_vasculitis_code_icd) |
    apcs.secondary_diagnosis.is_in(iga_vasculitis_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

iga_vasculitis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(iga_vasculitis_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(iga_vasculitis_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.iga_vasculitis = minimum_of(iga_vasculitis_primary_date,
                                    iga_vasculitis_secondary_date1,
                                    iga_vasculitis_secondary_date2)

# add date of polymyalgia rheumatica (pmr) onset column
pmr_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(pmr_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

pmr_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(pmr_code_icd) |
    apcs.secondary_diagnosis.is_in(pmr_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

pmr_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(pmr_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(pmr_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.polymyalgia_rheumatica = minimum_of(pmr_primary_date,
                                            pmr_secondary_date1,
                                            pmr_secondary_date2)

# add date of any group6 AI outcome onset column
dataset.grp6_outcome = minimum_of(dataset.anca_associated,
                                  dataset.giant_cell_arteritis,
                                  dataset.iga_vasculitis,
                                  dataset.polymyalgia_rheumatica)

# add date of immune thrombocytopenia onset column
immune_thromb_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(immune_thromb_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

immune_thromb_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(immune_thromb_code_icd) |
    apcs.secondary_diagnosis.is_in(immune_thromb_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

immune_thromb_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(immune_thromb_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(immune_thromb_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.immune_thrombocytopenia = minimum_of(immune_thromb_primary_date,
                                             immune_thromb_secondary_date1,
                                             immune_thromb_secondary_date2)

# add date of pernicious anaemia onset column
pernicious_anaemia_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(pernicious_anaemia_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

pernicious_anaemia_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(pernicious_anaemia_code_icd) |
    apcs.secondary_diagnosis.is_in(pernicious_anaemia_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

pernicious_anaemia_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(pernicious_anaemia_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(pernicious_anaemia_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.pernicious_anaemia = minimum_of(pernicious_anaemia_primary_date,
                                        pernicious_anaemia_secondary_date1,
                                        pernicious_anaemia_secondary_date2)

# add date of aplastic anaemia onset column
apa_primary_date = (clinical_events.where(
    (clinical_events.snomedct_code.is_in(apa_code_snomed) |
    clinical_events.ctv3_code.is_in(apa_code_ctv))
).sort_by(clinical_events.date).first_for_patient().date)

apa_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(apa_code_icd) |
    apcs.secondary_diagnosis.is_in(apa_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

apa_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(apa_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(apa_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.aplastic_anaemia = minimum_of(apa_primary_date,
                                      apa_secondary_date1,
                                      apa_secondary_date2)

# add date of autoimmune haemolytic anaemia onset column
aha_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(aha_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

aha_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(aha_code_icd) |
    apcs.secondary_diagnosis.is_in(aha_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

aha_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(aha_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(aha_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.ah_anaemia = minimum_of(aha_primary_date,
                                aha_secondary_date1,
                                aha_secondary_date2)

# add date of any group7 AI outcome onset column
dataset.grp7_outcome = minimum_of(dataset.immune_thrombocytopenia,
                                  dataset.pernicious_anaemia,
                                  dataset.aplastic_anaemia,
                                  dataset.ah_anaemia)

# add date of guillain barré onset column
glb_primary_date = (clinical_events.where(
    clinical_events.ctv3_code.is_in(glb_code_ctv)
).sort_by(clinical_events.date).first_for_patient().date)

glb_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(glb_code_icd) |
    apcs.secondary_diagnosis.is_in(glb_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

glb_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(glb_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(glb_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.guillain_barre = minimum_of(glb_primary_date,
                                    glb_secondary_date1,
                                    glb_secondary_date2)

# add date of multiple sclerosis onset column
multiple_sclerosis_primary_date = (clinical_events.where(
    clinical_events.ctv3_code.is_in(multiple_sclerosis_code_ctv)
).sort_by(clinical_events.date).first_for_patient().date)

multiple_sclerosis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(multiple_sclerosis_code_icd) |
    apcs.secondary_diagnosis.is_in(multiple_sclerosis_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

multiple_sclerosis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(multiple_sclerosis_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(multiple_sclerosis_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.multiple_sclerosis = minimum_of(multiple_sclerosis_primary_date,
                                        multiple_sclerosis_secondary_date1,
                                        multiple_sclerosis_secondary_date2)

# add date of myasthenia gravis onset column
myasthenia_gravis_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(myasthenia_gravis_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

myasthenia_gravis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(myasthenia_gravis_code_icd) |
    apcs.secondary_diagnosis.is_in(myasthenia_gravis_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

myasthenia_gravis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(myasthenia_gravis_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(myasthenia_gravis_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.myasthenia_gravis_anaemia = minimum_of(myasthenia_gravis_primary_date,
                                               myasthenia_gravis_secondary_date1,
                                               myasthenia_gravis_secondary_date2)

# add date of longitudinal myelitis onset column
longit_myelitis_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(longit_myelitis_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

longit_myelitis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(longit_myelitis_code_icd) |
    apcs.secondary_diagnosis.is_in(longit_myelitis_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

longit_myelitis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(longit_myelitis_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(longit_myelitis_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.longitudinal_myelitis = minimum_of(longit_myelitis_primary_date,
                                           longit_myelitis_secondary_date1,
                                           longit_myelitis_secondary_date2)

# add date of clinically isolated syndrome onset column
cis_primary_date = (clinical_events.where(
    clinical_events.snomedct_code.is_in(cis_code_snomed)
).sort_by(clinical_events.date).first_for_patient().date)

cis_secondary_date1 = (apcs.where(
    (apcs.primary_diagnosis.is_in(cis_code_icd) |
    apcs.secondary_diagnosis.is_in(cis_code_icd))
).sort_by(apcs.admission_date).first_for_patient().admission_date)

cis_secondary_date2 = (opa_diag.where(
    (opa_diag.primary_diagnosis_code.is_in(cis_code_icd) |
    opa_diag.secondary_diagnosis_code_1.is_in(cis_code_icd))
).sort_by(opa_diag.appointment_date).first_for_patient().appointment_date)

dataset.clinically_isolated_syndrome = minimum_of(cis_primary_date,
                                                  cis_secondary_date1,
                                                  cis_secondary_date2)

# add date of any group8 AI outcome onset column
dataset.grp8_outcome = minimum_of(dataset.guillain_barre,
                                  dataset.multiple_sclerosis,
                                  dataset.myasthenia_gravis_anaemia,
                                  dataset.longitudinal_myelitis,
                                  dataset.clinically_isolated_syndrome)

# add date of any AI outcome onset column
dataset.composite_ai_outcome = minimum_of(dataset.grp1_outcome,
                                          dataset.grp2_outcome,
                                          dataset.grp3_outcome,
                                          dataset.grp4_outcome,
                                          dataset.grp5_outcome,
                                          dataset.grp6_outcome,
                                          dataset.grp7_outcome,
                                          dataset.grp8_outcome)


## add binary variable for outcomes before start date
# create function for generating "history_of" variables
def history_of_ctv(column_name, ctv3code, icd_code):

  dataset.add_column(column_name, (    
      (clinical_events.where(
          (clinical_events.ctv3_code.is_in(ctv3code)) &
      (clinical_events.date.is_before(start_date))
      ).exists_for_patient()) |
      (apcs.where(
          ((apcs.primary_diagnosis.is_in(icd_code)) | 
          (apcs.secondary_diagnosis.is_in(icd_code))) &
          (apcs.admission_date.is_before(start_date))
      ).exists_for_patient()) |
      (opa_diag.where(
          ((opa_diag.primary_diagnosis_code.is_in(icd_code)) | 
          (opa_diag.secondary_diagnosis_code_1.is_in(icd_code))) &
          (opa_diag.appointment_date.is_before(start_date))
      ).exists_for_patient())
  )
  )

def history_of_snomed(column_name, snomedcode, icd_code):

  dataset.add_column(column_name, (    
      (clinical_events.where(
          (clinical_events.snomedct_code.is_in(snomedcode)) &
      (clinical_events.date.is_before(start_date))
      ).exists_for_patient()) |
      (apcs.where(
          ((apcs.primary_diagnosis.is_in(icd_code)) | 
          (apcs.secondary_diagnosis.is_in(icd_code))) &
          (apcs.admission_date.is_before(start_date))
      ).exists_for_patient()) |
      (opa_diag.where(
          ((opa_diag.primary_diagnosis_code.is_in(icd_code)) | 
          (opa_diag.secondary_diagnosis_code_1.is_in(icd_code))) &
          (opa_diag.appointment_date.is_before(start_date))
      ).exists_for_patient())
  )
  )

history_of_ctv("hydra_supp_hist1", hs_code_ctv, hs_code_icd)
history_of_ctv("crohn_disease_hist1", crohn_code_ctv, crohn_code_icd)


## add history of group 1 outcomes
# history of Rheumatoid Arthritis
history_of_snomed("rheu_arth_hist", ra_code_snomed, ra_code_icd)

# history of Undifferentiated Inflammatory Arthritis
dataset.undiff_eia_hist = (    
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(undiff_eia_code_snomed)) &
    (clinical_events.date.is_before(start_date))
    ).exists_for_patient())) 

# history of Psoriatic Arthritis
history_of_snomed("psor_arth_hist", psoa_code_snomed, psoa_code_icd) 

# history of Axial Spondyloarthritis
history_of_snomed("axial_arth_hist", axial_code_snomed, axial_code_icd) 

# history of any group1 outcome
dataset.grp1_outcome_hist = (dataset.rheu_arth_hist |
                             dataset.undiff_eia_hist |
                             dataset.psor_arth_hist |
                             dataset.axial_arth_hist)


## add history of group2 outcomes
# add history of systemic lupus erythematosus (sle) onset column
history_of_ctv("sle_hist", sle_code_ctv, sle_code_icd)

# add history of sjogren's syndrome onset column
history_of_snomed("sjogren_hist", sjs_code_snomed, sjs_code_icd)

# add history of systemic sclerosis/scleroderma (sys_sclerosis) onset column
history_of_snomed("sys_scle_hist", sss_code_snomed, sss_code_icd)

# add hist of inflammatory myositis/polymyositis/dermatolomyositis (infl_myositis) onset column
history_of_snomed("infl_myo_hist", im_code_snomed, im_code_icd)

# add history of mixed connective tissue disease (mctd) onset column
history_of_snomed("mctd_hist", mctd_code_snomed, mctd_code_icd)

# add history of antiphospholipid syndrome onset column
dataset.antiphos_hist = (    
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(as_code_snomed)) &
    (clinical_events.date.is_before(start_date))
    ).exists_for_patient())
)

# add history of any group2 AI outcome onset column
dataset.grp2_outcome_hist = (dataset.sle_hist |
                             dataset.sjogren_hist |
                             dataset.sys_scle_hist |
                             dataset.infl_myo_hist |
                             dataset.mctd_hist |
                             dataset.antiphos_hist)


## add history of group3 outcomes
# add history of Psoriasis onset column
history_of_ctv("psoriasis_hist", psoriasis_code_ctv, psoriasis_code_icd)

# add history of Hydradenitis Suppurativa onset column
history_of_ctv("hydra_supp_hist", hs_code_ctv, hs_code_icd)

# add history of any group3 AI outcome onset column
dataset.grp3_outcome_hist = (dataset.psoriasis_hist |
                             dataset.hydra_supp_hist)


## add history of group4 outcomes
# add history of Crohn's disease column
history_of_ctv("crohn_hist", crohn_code_ctv, crohn_code_icd)

# add history of Ulcerative Colitis column
history_of_ctv("ulc_col_hist", uc_code_ctv, uc_code_icd)

# add history of Celiac Disease column
history_of_snomed("celiac_hist", celiac_code_snomed, celiac_code_icd)

# add history of Inflammatory Bowel Disease column
dataset.ibd_hist = (    
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(ibd_code_snomed) |
    clinical_events.ctv3_code.is_in(ibd_code_ctv3)) &
    (clinical_events.date.is_before(start_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(ibd_code_icd)) | 
        (apcs.secondary_diagnosis.is_in(ibd_code_icd))) &
        (apcs.admission_date.is_before(start_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(ibd_code_icd)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(ibd_code_icd))) &
        (opa_diag.appointment_date.is_before(start_date))
    ).exists_for_patient())
)

# add history of any group4 AI outcome onset column
dataset.grp4_outcome_hist = (dataset.crohn_hist |
                             dataset.ulc_col_hist |
                             dataset.celiac_hist |
                             dataset.ibd_hist)

## add history of group 5 outcomes
# add history of Addison's disease column
history_of_snomed("addison_hist", addison_code_snomed, addison_code_icd)

# add history of Grave's disease column
history_of_snomed("grave_hist", grave_code_snomed, grave_code_icd)

# add history of Hashimoto's thyroiditis column
history_of_snomed("hashimoto_hist", hashimoto_thyroiditis_code_snomed, hashimoto_thyroiditis_code_icd)

# add history of any group5 AI outcome column
dataset.grp5_outcome_hist = (dataset.addison_hist |
                             dataset.grave_hist |
                             dataset.hashimoto_hist)


## add history of group 6 outcomes
# add history of ANCA-Associated column
history_of_snomed("anca_hist", anca_code_snomed, anca_code_icd)

# add history of Giant Cell Arteritis column
history_of_snomed("gca_hist", gca_code_snomed, gca_code_icd)

# add history of Immunoglobin A Vasculitis column
history_of_snomed("iga_vasc_hist", iga_vasculitis_code_snomed, iga_vasculitis_code_icd)

# add history of Polymyalgia Rheumatica column
history_of_snomed("pmr_hist", pmr_code_snomed, pmr_code_icd)

# add history of any group6 AI outcome onset column
dataset.grp6_outcome_hist = (dataset.anca_hist |
                             dataset.gca_hist |
                             dataset.iga_vasc_hist |
                             dataset.pmr_hist)

## add history of group 7 outcomes
# add history of Immune Thrombocytopenia column
history_of_snomed("imm_thromb_hist", immune_thromb_code_snomed, immune_thromb_code_icd)

# add history of Pernicious Anaemia column
history_of_snomed("pern_anaem_hist", pernicious_anaemia_code_snomed, pernicious_anaemia_code_icd)

# add history of Aplastic Anaemia onset column
dataset.aplast_anaem_hist = (    
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(apa_code_snomed) |
         clinical_events.ctv3_code.is_in(apa_code_ctv)) &
    (clinical_events.date.is_before(start_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(apa_code_icd)) | 
        (apcs.secondary_diagnosis.is_in(apa_code_icd))) &
        (apcs.admission_date.is_before(start_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(apa_code_icd)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(apa_code_icd))) &
        (opa_diag.appointment_date.is_before(start_date))
    ).exists_for_patient())
)

# add history of autoimmune haemolytic anaemia column
history_of_snomed("auto_haem_anaem_hist", aha_code_snomed, aha_code_icd)

# add history of any group7 AI outcome column
dataset.grp7_outcome_hist = (dataset.imm_thromb_hist |
                             dataset.pern_anaem_hist |
                             dataset.aplast_anaem_hist |
                             dataset.auto_haem_anaem_hist)


## add history of group 8 outcomes
# add history of Guillain-Barré column
history_of_ctv("guil_bar_hist", glb_code_ctv, glb_code_icd)

# add history of Multiple Sclerosis column
history_of_ctv("mult_scler_hist", multiple_sclerosis_code_ctv, multiple_sclerosis_code_icd)

# add history of Myasthenia Gravis column
history_of_snomed("myas_grav_hist", myasthenia_gravis_code_snomed, myasthenia_gravis_code_icd)

# add history of Longitudinal Myelitis column
history_of_snomed("long_myel_hist", longit_myelitis_code_snomed, longit_myelitis_code_icd)

# add history of Clinically Isolated Syndrome column
history_of_snomed("clin_isol_hist", cis_code_snomed, cis_code_icd)

# add history of any group8 AI outcome onset column
dataset.grp8_outcome_hist = (dataset.guil_bar_hist |
                             dataset.mult_scler_hist |
                             dataset.myas_grav_hist |
                             dataset.long_myel_hist |
                             dataset.clin_isol_hist)


## add history of any AI outcome onset column
dataset.composite_ai_outcome_hist = (dataset.grp1_outcome_hist |
                                     dataset.grp2_outcome_hist |
                                     dataset.grp3_outcome_hist |
                                     dataset.grp4_outcome_hist |
                                     dataset.grp5_outcome_hist |
                                     dataset.grp6_outcome_hist |
                                     dataset.grp7_outcome_hist |
                                     dataset.grp8_outcome_hist)
