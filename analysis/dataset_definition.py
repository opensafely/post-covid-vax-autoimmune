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
    medications
)

# import codelists
from codelists import *

# create dataset
dataset = create_dataset()

# define index date
index_date = "2020-03-31"

# prior follow-up date
prior_followup_date = "2019-09-30"

## defining inclusion criteria
# find patients who are have 6 month prior follow-up to index date
has_registration = practice_registrations.for_patient_on(
    prior_followup_date
).exists_for_patient()

# find patients with known deprivation
has_deprivation_index = addresses.for_patient_on(
    index_date
).imd_rounded.is_not_null()

# find patients with known region
has_region = practice_registrations.for_patient_on(
    index_date
).practice_nuts1_region_name.is_not_null()


## create dataset with inclusion criteria
dataset.define_population(
    (patients.date_of_birth.is_on_or_before("2002-03-31")) & 
    (patients.date_of_birth.is_on_or_after("1910-03-31")) & 
    (has_registration) &
    (patients.sex != "unknown") &
    (has_deprivation_index) &
    (has_region))


## add potential confounders to dataset
# add patients date of birth as column
dataset.dob = patients.date_of_birth

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
imd_rounded = addresses.for_patient_on(index_date).imd_rounded
max_imd = 32844

imd_decile = case(
    when(imd_rounded < int(max_imd/10)).then(1),
    when(imd_rounded < int(max_imd/5)).then(2),
    when(imd_rounded < int(max_imd*(3/10))).then(3),
    when(imd_rounded < int(max_imd*(2/5))).then(4),
    when(imd_rounded < int(max_imd/2)).then(5),
    when(imd_rounded < int(max_imd*(3/5))).then(6),
    when(imd_rounded < int(max_imd*(7/10))).then(7),
    when(imd_rounded < int(max_imd*(4/5))).then(8),
    when(imd_rounded < int(max_imd*(9/10))).then(9),
    when(imd_rounded <= max_imd).then(10),
)

dataset.imd_decile = imd_decile

# add region column
region = (practice_registrations.for_patient_on(index_date)
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
    ]) & appointments.start_date.is_on_or_between(index_date - days(365), index_date)
).count_for_patient()

# add smoking status column
dataset.smoking_status = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(smoking_clear)
    )
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
    addresses.for_patient_on(index_date).care_home_is_potential_match |
    addresses.for_patient_on(index_date).care_home_requires_nursing |
    addresses.for_patient_on(index_date).care_home_does_not_require_nursing
)

# add dementia column
dataset.dementia = (    
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(
            dementia_snomed_clinical + dementia_vascular_snomed_clinical)) &
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(dementia_icd10 + dementia_vascular_icd10)) | 
        (apcs.secondary_diagnosis.is_in(dementia_icd10 + dementia_vascular_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(dementia_icd10 + dementia_vascular_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(dementia_icd10 + dementia_vascular_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add liver disease column
dataset.liver_disease = (    
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(liver_disease_snomed_clinical)) &
    (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(liver_disease_icd10)) | 
        (apcs.secondary_diagnosis.is_in(liver_disease_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(liver_disease_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(liver_disease_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add chonic kidney disease column
dataset.chronic_kidney_disease = (
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(ckd_snomed_clinical)) &
    (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(ckd_icd10)) |
        (apcs.secondary_diagnosis.is_in(ckd_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(ckd_icd10)) |
        (opa_diag.secondary_diagnosis_code_1.is_in(ckd_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add cancer column
dataset.cancer = (        
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(cancer_snomed_clinical)) &
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(cancer_icd10)) | 
        (apcs.secondary_diagnosis.is_in(cancer_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
        (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(cancer_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(cancer_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add hypertention column
dataset.hypertension = (
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(hypertension_snomed_clinical)) &
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (medications.where(
        ((medications.dmd_code.is_in(hypertension_drugs_dmd))) &
        (medications.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(hypertension_icd10)) |
        (apcs.secondary_diagnosis.is_in(hypertension_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
            (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(hypertension_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(hypertension_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
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
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (medications.where(
        ((medications.dmd_code.is_in(insulin_snomed_clinical)) | 
        (medications.dmd_code.is_in(antidiabetic_drugs_snomed_clinical)) |
        (medications.dmd_code.is_in(non_metformin_dmd))) &
        (medications.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(diabetes_type1_icd10)) | 
        (apcs.primary_diagnosis.is_in(diabetes_type2_icd10)) |
        (apcs.secondary_diagnosis.is_in(diabetes_type1_icd10)) |
        (apcs.secondary_diagnosis.is_in(diabetes_type2_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(diabetes_type1_icd10)) | 
        (opa_diag.primary_diagnosis_code.is_in(diabetes_type2_icd10)) |
        (opa_diag.secondary_diagnosis_code_1.is_in(diabetes_type1_icd10)) |
        (opa_diag.secondary_diagnosis_code_1.is_in(diabetes_type2_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add obesity column
dataset.obesity = (
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(bmi_obesity_snomed_clinical)) &
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(bmi_obesity_icd10)) |
        (apcs.secondary_diagnosis.is_in(bmi_obesity_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(bmi_obesity_icd10)) |
        (opa_diag.secondary_diagnosis_code_1.is_in(bmi_obesity_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add chronic obstructive pulmonary diease (copd) column
dataset.copd = (
    (clinical_events.where(
        ((clinical_events.snomedct_code.is_in(copd_snomed_clinical))) &
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(copd_icd10)) |
        (apcs.secondary_diagnosis.is_in(copd_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(copd_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(copd_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add acute myocardial infarction (ami) column
dataset.ami = (        
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(ami_snomed_clinical)) &
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(ami_icd10 + ami_prior_icd10)) | 
        (apcs.secondary_diagnosis.is_in(ami_icd10 + ami_prior_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(ami_icd10 + ami_prior_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(ami_icd10 + ami_prior_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)

# add ischaemic stroke column
dataset.ischaemic_stroke = (        
    (clinical_events.where(
        (clinical_events.snomedct_code.is_in(stroke_isch_snomed_clinical)) &
        (clinical_events.date.is_before(index_date))
    ).exists_for_patient()) |
    (apcs.where(
        ((apcs.primary_diagnosis.is_in(stroke_isch_icd10)) | 
        (apcs.secondary_diagnosis.is_in(stroke_isch_icd10))) &
        (apcs.admission_date.is_before(index_date))
    ).exists_for_patient()) |
    (opa_diag.where(
        ((opa_diag.primary_diagnosis_code.is_in(stroke_isch_icd10)) | 
        (opa_diag.secondary_diagnosis_code_1.is_in(stroke_isch_icd10))) &
        (opa_diag.appointment_date.is_before(index_date))
    ).exists_for_patient())
)


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
dataset.date_covid19_vax1 = (clinical_events.where(
    clinical_events.snomedct_code.is_in(vac_adm_1)
).sort_by(clinical_events.date).first_for_patient().date)

dataset.date_covid19_vax2 = (clinical_events.where(
    clinical_events.snomedct_code.is_in(vac_adm_2)
).sort_by(clinical_events.date).first_for_patient().date)

dataset.date_covid19_vax3 = (clinical_events.where(
    clinical_events.snomedct_code.is_in(vac_adm_3)
).sort_by(clinical_events.date).first_for_patient().date)

dataset.date_covid19_vax4 = (clinical_events.where(
    clinical_events.snomedct_code.is_in(vac_adm_4)
).sort_by(clinical_events.date).first_for_patient().date)

dataset.date_covid19_vax5 = (clinical_events.where(
    clinical_events.snomedct_code.is_in(vac_adm_5)
).sort_by(clinical_events.date).first_for_patient().date)


## add outcomes to the dataset
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

dataset.rheuatoid_arthritis_date = minimum_of(rheumatoid_arthritis_primary_date,
                                              rheumatoid_arthritis_secondary_date1,
                                              rheumatoid_arthritis_secondary_date2)

# add date of undifferentiated inflammatory arthritis (undiff_eia) onset column
dataset.undiff_eia_date = (clinical_events.where(
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

