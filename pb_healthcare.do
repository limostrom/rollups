



cap cd "$proj_dir"
pause on

import delimited "processed-data/states.csv", clear varn(1)
	ren statename com_hqstate_province
	ren statefips statefips_fill
	tempfile states
	save `states', replace
	

* -------------- Breakdown of Platforms Within Opaque Groupings ----------------
use "processed-data/pitchbook_bycbsa.dta", clear
keep dealid dealdate companyid companyname addonplatform platformid ///
	primaryindustrygroup primaryindustrycode cbsacode statefips com_hqstate
drop if addonplatform == ""
duplicates drop

gen year = real(substr(dealdate, -4, .))

egen dealid_num = group(dealid)
bys platformid: egen modal_indgrp = mode(primaryindustrygroup)
bys platformid: gen deals = _N
	

keep if modal_indgrp == "Healthcare Services" | modal_indgrp == "Services (Non-Financial)"

#delimit ;
gen ind_laurenassigned = "Specialists (Anesthesiology)"
	if strpos(addonplatform, "Anesthesi") > 0;
replace ind_laurenassigned = "Specialists (Audiology)"
	if strpos(addonplatform, "Audiolog") > 0;
replace ind_laurenassigned = "Specialists (Dental & Orthodontic)"
	if strpos(addonplatform, "Dent") > 0 
		| strpos(addonplatform, "Orthodont") > 0
		| inlist(addonplatform, "Smile Doctors", "Cordental Group", "Smile Brands",
		"Smile Partners USA");
replace ind_laurenassigned = "Specialists (Dermatology)"
	if strpos(addonplatform, "Dermatolog") > 0
	| inlist(addonplatform, "QualDerm Partners");
replace ind_laurenassigned = "Specialists (Gastroenterology)"
	if strpos(addonplatform, "Gastro") > 0
		| strpos(addonplatform, "Digestive") > 0;
replace ind_laurenassigned = "Specialists (Mammography & OB/GYN)"
	if inlist(addonplatform, "Axia Women's Health", "Solis Mammography");
replace ind_laurenassigned = "Specialists (Optometry)"
	if strpos(addonplatform, "Eye") > 0 
		| strpos(addonplatform, "Vision") > 0
		| inlist(addonplatform, "NJRetina", "SightMD", "Retina Consultants of America");
replace ind_laurenassigned = "Specialists (Pain Management)"
	if inlist(addonplatform, "Prospira PainCare");
replace ind_laurenassigned = "Specialists (Podiatry)"
	if inlist(addonplatform, "Foot & Ankle Specialists of the Mid-Atlantic");
replace ind_laurenassigned = "Specialists (Surgery)"
	if inlist(addonplatform, "National Surgical Care", "SpecialtyCare");
replace ind_laurenassigned = "Physical Therapy & Rehab"
	if strpos(addonplatform, "Physical Therapy") > 0 
		| inlist(addonplatform, "Ivy Rehab", "CBI Health Group", "CORA Health Services",
		"PT Solutions", "Physical Rehabilitation Network", "Pivot Health Solutions",
		"Therapy Partners Group", "Upstream Rehabilitation",
		"Phoenix Rehabilitation and Health Services")
		| inlist(addonplatform, "One Call Care Management");
replace ind_laurenassigned = "Lab Services & Diagnostic Imaging"
	if inlist(addonplatform, "Aurora Diagnostics", "Rayus Radiology");
replace ind_laurenassigned = "Addiction & Mental Health (In- or Outpatient)"
	if inlist(addonplatform, "BayMark Health Services", "Acadia Healthcare",
		"Behavioral Health Group", "CRC Health Group", "Discovery Behavioral Health",
		"Refresh Mental Health", "Pinnacle Treatment Centers", "Pyramid Healthcare")
		| inlist(addonplatform, "Sequel Youth and Family Services",
		"Summit Behavioral Healthcare");
replace ind_laurenassigned = "Primary Care / Urgent Care"
	if inlist(addonplatform, "Concentra", "U.S. HealthWorks", "Duly Health and Care",
		"Sound Physicians", "Team Health Holdings");
replace ind_laurenassigned = "Pharmacies"
	if inlist(addonplatform, "Rubicon Pharmacies");
replace ind_laurenassigned = "Elder & Disabled Care"
	if inlist(addonplatform, "Encompass Home Health", "Jordan Health Services",
		"Sevita", "Active Day (Pennsylvania)", "AccentCare", "Acorn Health", "Arosa",
		"Aveanna Healthcare", "BrightSpring Health Services")
		| inlist(addonplatform, "Bristol Hospice", "Care Advantage", "Care Hospice",
		"CareSouth Health System", "Caregiver", "Cornerstone Healthcare Group",
		"HealthPRO Heritage", "St. Croix Hospice", "Traditions Health");
replace ind_laurenassigned = "Veterinarians"
	if strpos(addonplatform, "Veterin") > 0
		| inlist(addonplatform, "Innovetive Petcare", "People, Pets & Vets", "PetVet Care Centers");
replace ind_laurenassigned = "Infusion/Specialty Pharmacy"
	if inlist(addonplatform, "CarePoint Partners", "BriovaRx Infusion Services");
replace ind_laurenassigned = "Medical Aesthetics Clinics"
	if inlist(addonplatform, "MedSpa Partners");
replace ind_laurenassigned = "Healthcare Business Services / Plan Administration"
	if inlist(addonplatform, "Cloudmed", "Enlyte", "Genex Services", "Epic Health Services",
		"HealthSmart");
replace ind_laurenassigned = "Clinical Trials"
	if inlist(addonplatform, "CenExel Clinical Research", "Velocity Clinical Research");
replace ind_laurenassigned = "Emergency Transport"
	if inlist(addonplatform, "Global Medical Response", "Priority Ambulance");
replace ind_laurenassigned = "Long-Term Care Pharmacy"
	if inlist(addonplatform, "Guardian Pharmacy Services");
	
	
replace ind_laurenassigned = "Preschool/Childcare"
	if inlist(addonplatform, "Cadence Education", "Endeavor Schools");
replace ind_laurenassigned = "Educational Consulting"
	if inlist(addonplatform, "Catapult Learning");
replace ind_laurenassigned = "Trade & Vocational Schools"
	if inlist(addonplatform, "Lincoln Educational Services ( New Jersey)");
replace ind_laurenassigned = "Education Travel"
	if inlist(addonplatform, "WorldStrides");
replace ind_laurenassigned = "Property Management"
	if inlist(addonplatform, "HomeRiver Group", "Valet Living");
replace ind_laurenassigned = "HVAC & Plumbing"
	if inlist(addonplatform, "NearU Services", "Right Time Heating and Air Conditioning Canada",
		"Service Champions", "Southern HVAC", "Strikepoint Group Holding", "The Wrench Group",
		"TurnPoint Services");
#delimit cr

preserve // top 1% largest platforms
	keep if deals >= 24
	keep addonplatform deals ind_laurenassigned
	duplicates drop
	gsort ind_laurenassigned

	graph hbar (sum) deals, over(ind_laurenassigned, sort(1) descending label(labsize(small))) ///
		yti("Number of Deals" "(Among Top Percentile Largest Healthcare Platforms)", size(small))
		
	graph export "output/bar_healthcare_platforms_top1pct.pdf", replace as(pdf)
restore

preserve // top 5% largest platforms
	keep if deals >= 10
	keep if !inlist(ind_laurenassigned, "Preschool/Childcare", "Educational Consulting", ///
	"Trade & Vocational Schools", "Education Travel", "Property Management", "HVAC & Plumbing")
	keep addonplatform deals ind_laurenassigned
	duplicates drop
	gsort ind_laurenassigned

	graph hbar (sum) deals, over(ind_laurenassigned, sort(1) descending label(labsize(small))) ///
		yti("Number of Deals" "(Among Top Percentile Largest Healthcare Platforms)", size(small))
		
	graph export "output/bar_healthcare_platforms_top5pct.pdf", replace as(pdf)
restore


* By CBSA
preserve
	keep if deals >= 10
	drop if cbsacode == .
	collapse (last) deals modal_indgrp ind_laurenassigned ///
			 (count) deals_same_cbsa = dealid_num, ///
		by(platformid addonplatform cbsacode)
		
	gen sh_deals_same_cbsa = deals_same_cbsa / deals
	bys platformid: egen rank_cbsa = rank(sh_deals_same_cbsa), field
	gsort -deals addonplatform rank_cbsa
	br addonplatform deals cbsacode deals_same_cbsa sh_deals_same_cbsa rank_cbsa if rank_cbsa < 5
restore



* By State
preserve
	keep if deals >= 10
	merge m:1 com_hqstate_province using `states', keep(1 3) nogen keepus(statefips_fill)
		replace statefips = statefips_fill if statefips == . 

	drop if statefips == .
	collapse (last) deals ind_laurenassigned (count) deals_same_st = dealid_num, ///
		by(platformid addonplatform statefips com_hqstate)
		
	gen sh_deals_same_st = deals_same_st / deals
		replace sh_deals_same_st = int(sh_deals_same_st*1000)/10
	bys platformid: egen rank_st = rank(sh_deals_same_st), field
	gsort -sh_deals_same_st addonplatform
	br ind_laurenassigned addonplatform deals com_hqstate deals_same_st sh_deals_same_st rank_st ///
		if rank_st == 1
restore

* Moments of distribution by primary, specialty, or inpatient
preserve
	keep if deals >= 10
	replace ind_laurenassigned = "Primary, Dental, Vision, PT, EMS" ///
		if inlist(ind_laurenassigned, "Primary Care / Urgent Care", ///
			"Specialists (Dental & Orthodontic)", "Specialists (Optometry)", ///
			"Physical Therapy & Rehab", "Emergency Transport")
	replace ind_laurenassigned = "Labs & Pharmacies" ///
		if inlist(ind_laurenassigned, "Lab Services & Diagnostic Imaging", ///
			"Pharmacies")
	replace ind_laurenassigned = "Specialists & Clinical Trials" ///
		if substr(ind_laurenassigned, 1, 11) == "Specialists" ///
			| inlist(ind_laurenassigned, "Infusion/Specialty Pharmacy", ///
			"Medical Aesthetics Clinics", "Clinical Trials")
	replace ind_laurenassigned = "Elder/Disabled Care and Behavioral" ///
		if inlist(ind_laurenassigned, "Elder & Disabled Care", ///
			"Addiction & Mental Health (In- or Outpatient)", "Long-Term Care Pharmacy") 
			
	bys platformid statefips: gen deals_same_st = _N
	bys platformid cbsacode: gen deals_same_cbsa = _N
	bys platformid: gen total_deals = _N
	
	drop if cbsacode == . | statefips == .
	bys platformid: egen deals_top_state = max(deals_same_st)
	bys platformid: egen deals_top_cbsa = max(deals_same_cbsa)
	
	keep platformid ind_laurenassigned total_deals deals_top_state deals_top_cbsa
	duplicates drop
	
	gen top_state_sh = deals_top_state / total_deals
		replace top_state_sh = int(top_state_sh*1000)/10
	gen top_cbsa_sh = deals_top_cbsa / total_deals
		replace top_cbsa_sh = int(top_cbsa_sh*1000)/10
	
	collapse (mean) top_state_sh_mean = top_state_sh top_cbsa_sh_mean = top_cbsa_sh deals_mean = total_deals ///
			 (sd) top_state_sh_sd = top_state_sh top_cbsa_sh_sd = top_cbsa_sh deals_sd = total_deals ///
			 (iqr) top_state_sh_iqr = top_state_sh top_cbsa_sh_iqr = top_cbsa_sh deals_iqr = total_deals ///
			 (count) platforms = platformid ///
		, by(ind_laurenassigned)
		
	order ind_laurenassigned platforms deals_mean deals_sd deals_iqr ///
			top_cbsa_sh_mean top_cbsa_sh_sd top_cbsa_sh_iqr ///
			top_state_sh_mean top_state_sh_sd top_state_sh_iqr
br if !inlist(ind_laurenassigned, "Preschool/Childcare", "Educational Consulting", ///
	"Trade & Vocational Schools", "Education Travel", "Property Management", "HVAC & Plumbing")
pause
restore


