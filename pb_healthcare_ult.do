



cap cd "$proj_dir"
pause on

import delimited "processed-data/states.csv", clear varn(1)
	ren statename com_hqstate_province
	ren statefips statefips_fill
	tempfile states
	save `states', replace
	

* -------------- Breakdown of Platforms Within Opaque Groupings ----------------
use "processed-data/pitchbook_bycz.dta", clear
	merge m:1 addonplatform using "processed-data/layered_rollups_pb.dta", nogen assert(3)
keep dealid dealdate companyid companyname addonplatform platformid ultimate_platform ///
	primaryindustrygroup primaryindustrycode cz statefips com_hqstate
drop if addonplatform == ""
duplicates drop

gen year = real(substr(dealdate, -4, .))

egen dealid_num = group(dealid)
bys ultimate_platform: egen modal_indgrp = mode(primaryindustrygroup)
bys ultimate_platform: gen deals = _N
	

keep if modal_indgrp == "Healthcare Services" | modal_indgrp == "Services (Non-Financial)"

#delimit ;
gen ind_laurenassigned = "Specialists (Anesthesiology)"
	if strpos(ultimate_platform, "Anesthesi") > 0;
replace ind_laurenassigned = "Specialists (Audiology)"
	if strpos(ultimate_platform, "Audiolog") > 0;
replace ind_laurenassigned = "Specialists (Dental & Orthodontic)"
	if strpos(ultimate_platform, "Dent") > 0 
		| strpos(ultimate_platform, "Orthodont") > 0
		| inlist(ultimate_platform, "Smile Doctors", "Cordental Group", "Smile Brands",
		"Smile Partners USA");
replace ind_laurenassigned = "Specialists (Dermatology)"
	if strpos(ultimate_platform, "Dermatolog") > 0
	| inlist(ultimate_platform, "QualDerm Partners");
replace ind_laurenassigned = "Specialists (Gastroenterology)"
	if strpos(ultimate_platform, "Gastro") > 0
		| strpos(ultimate_platform, "Digestive") > 0;
replace ind_laurenassigned = "Specialists (Mammography & OB/GYN)"
	if inlist(ultimate_platform, "Axia Women's Health", "Solis Mammography");
replace ind_laurenassigned = "Specialists (Optometry)"
	if strpos(ultimate_platform, "Eye") > 0 
		| strpos(ultimate_platform, "Vision") > 0
		| inlist(ultimate_platform, "NJRetina", "SightMD", "Retina Consultants of America");
replace ind_laurenassigned = "Specialists (Pain Management)"
	if inlist(ultimate_platform, "Prospira PainCare", "National Spine & Pain Centers");
replace ind_laurenassigned = "Specialists (Podiatry)"
	if inlist(ultimate_platform, "Foot & Ankle Specialists of the Mid-Atlantic");
replace ind_laurenassigned = "Specialists (Surgery)"
	if inlist(ultimate_platform, "National Surgical Care", "SpecialtyCare");
replace ind_laurenassigned = "Physical Therapy & Rehab"
	if strpos(ultimate_platform, "Physical Therapy") > 0 
		| inlist(ultimate_platform, "Ivy Rehab", "CBI Health Group", "CORA Health Services",
		"PT Solutions", "Physical Rehabilitation Network", "Pivot Health Solutions",
		"Therapy Partners Group", "Upstream Rehabilitation",
		"Phoenix Rehabilitation and Health Services")
		| inlist(ultimate_platform, "One Call Care Management", "LifePoint Health");
replace ind_laurenassigned = "Lab Services & Diagnostic Imaging"
	if inlist(ultimate_platform, "Aurora Diagnostics", "Rayus Radiology");
replace ind_laurenassigned = "Addiction & Mental Health (In- or Outpatient)"
	if inlist(ultimate_platform, "BayMark Health Services", "Acadia Healthcare",
		"Behavioral Health Group", "CRC Health Group", "Discovery Behavioral Health",
		"Refresh Mental Health", "Pinnacle Treatment Centers", "Pyramid Healthcare")
		| inlist(ultimate_platform, "Sequel Youth and Family Services",
		"Summit Behavioral Healthcare");
replace ind_laurenassigned = "Primary Care / Urgent Care"
	if inlist(ultimate_platform, "Concentra", "U.S. HealthWorks", "Duly Health and Care",
		"Sound Physicians", "Team Health Holdings", "US Acute Care Solutions",
		"Steward Health Care", "Envision Healthcare", "Ardent Health Services");
replace ind_laurenassigned = "Pharmacies"
	if inlist(ultimate_platform, "Rubicon Pharmacies");
replace ind_laurenassigned = "Elder & Disabled Care"
	if inlist(ultimate_platform, "Encompass Home Health", "Jordan Health Services",
		"Sevita", "Active Day (Pennsylvania)", "AccentCare", "Acorn Health", "Arosa",
		"Aveanna Healthcare", "BrightSpring Health Services")
		| inlist(ultimate_platform, "Bristol Hospice", "Care Advantage", "Care Hospice",
		"CareSouth Health System", "Caregiver", "Cornerstone Healthcare Group",
		"HealthPRO Heritage", "St. Croix Hospice", "Traditions Health")
		| inlist(ultimate_platform, "PharMerica", "Elara Caring");
replace ind_laurenassigned = "Veterinarians"
	if strpos(ultimate_platform, "Veterin") > 0
		| inlist(ultimate_platform, "Innovetive Petcare", "People, Pets & Vets",
		"PetVet Care Centers", "Compassion-First Pet Hospitals", "VetCor");
replace ind_laurenassigned = "Infusion/Specialty Pharmacy"
	if inlist(ultimate_platform, "CarePoint Partners", "BriovaRx Infusion Services",
		"AdaptHealth");
replace ind_laurenassigned = "Medical Aesthetics Clinics"
	if inlist(ultimate_platform, "MedSpa Partners");
replace ind_laurenassigned = "Healthcare Business Services / Plan Administration"
	if inlist(ultimate_platform, "Cloudmed", "Enlyte", "Genex Services", "Epic Health Services",
		"HealthSmart");
replace ind_laurenassigned = "Clinical Trials"
	if inlist(ultimate_platform, "CenExel Clinical Research", "Velocity Clinical Research");
replace ind_laurenassigned = "Emergency Transport"
	if inlist(ultimate_platform, "Global Medical Response", "Priority Ambulance");
replace ind_laurenassigned = "Long-Term Care Pharmacy"
	if inlist(ultimate_platform, "Guardian Pharmacy Services");
replace ind_laurenassigned = "Dialysis"
	if inlist(ultimate_platform, "Liberty Dialysis", "U.S. Renal Care");
replace ind_laurenassigned = "Medical Equipment"
	if inlist(ultimate_platform, "Alpha Source");
	
	
replace ind_laurenassigned = "Preschool/Childcare"
	if inlist(ultimate_platform, "Cadence Education", "Endeavor Schools");
replace ind_laurenassigned = "Educational Consulting"
	if inlist(ultimate_platform, "Catapult Learning");
replace ind_laurenassigned = "Trade & Vocational Schools"
	if inlist(ultimate_platform, "Lincoln Educational Services ( New Jersey)");
replace ind_laurenassigned = "Education Travel"
	if inlist(ultimate_platform, "WorldStrides");
replace ind_laurenassigned = "Property Management"
	if inlist(ultimate_platform, "HomeRiver Group", "Valet Living");
replace ind_laurenassigned = "HVAC & Plumbing"
	if inlist(ultimate_platform, "NearU Services", "Right Time Heating and Air Conditioning Canada",
		"Service Champions", "Southern HVAC", "Strikepoint Group Holding", "The Wrench Group",
		"TurnPoint Services");
#delimit cr

preserve // top 1% largest platforms
	keep if deals >= 29
	keep ultimate_platform deals ind_laurenassigned
	duplicates drop
	gsort ind_laurenassigned

	graph hbar (sum) deals, over(ind_laurenassigned, sort(1) descending label(labsize(small))) ///
		yti("Number of Deals" "(Among Top Percentile Largest Healthcare Platforms)", size(small))
		
	graph export "output/bar_healthcare_platforms_ult_top1pct.pdf", replace as(pdf)
restore

preserve // top 5% largest platforms
	keep if deals >= 12
	keep if !inlist(ind_laurenassigned, "Preschool/Childcare", "Educational Consulting", ///
	"Trade & Vocational Schools", "Education Travel", "Property Management", "HVAC & Plumbing")
	keep ultimate_platform deals ind_laurenassigned
	duplicates drop
	gsort ind_laurenassigned

	graph hbar (sum) deals, over(ind_laurenassigned, sort(1) descending label(labsize(small))) ///
		yti("Number of Deals" "(Among Top Percentile Largest Healthcare Platforms)", size(small))
		
	graph export "output/bar_healthcare_platforms_ult_top5pct.pdf", replace as(pdf)
restore



* By CZ (Top 1%)
preserve
	keep if deals >= 29
	drop if cz == .
	collapse (last) deals modal_indgrp ind_laurenassigned ///
			 (count) deals_same_cz = dealid_num, ///
		by(ultimate_platform cz)
		
	gen sh_deals_same_cz = deals_same_cz / deals
	replace sh_deals_same_cz = int(sh_deals_same_cz*1000)/10
	bys ultimate_platform: egen rank_cz = rank(sh_deals_same_cz), field
	gsort -sh_deals_same_cz ultimate_platform rank_cz
	br ultimate_platform deals cz deals_same_cz sh_deals_same_cz rank_cz if rank_cz == 1 
	pause
restore

* By CZ (Top 5%)
preserve
	keep if deals >= 12
	drop if cz == .
	collapse (last) deals modal_indgrp ind_laurenassigned ///
			 (count) deals_same_cz = dealid_num, ///
		by(ultimate_platform cz)
		
	gen sh_deals_same_cz = deals_same_cz / deals
	bys ultimate_platform: egen rank_cz = rank(sh_deals_same_cz), field
	gsort -sh_deals_same_cz ultimate_platform rank_cz
	br ultimate_platform deals cz deals_same_cz sh_deals_same_cz rank_cz if rank_cz == 1 
	pause
restore



* By State
preserve
	keep if deals >= 12
	merge m:1 com_hqstate_province using `states', keep(1 3) nogen keepus(statefips_fill)
		replace statefips = statefips_fill if statefips == . 

	drop if statefips == .
	collapse (last) deals ind_laurenassigned (count) deals_same_st = dealid_num, ///
		by(ultimate_platform statefips com_hqstate)
		
	gen sh_deals_same_st = deals_same_st / deals
		replace sh_deals_same_st = int(sh_deals_same_st*1000)/10
	bys ultimate_platform: egen rank_st = rank(sh_deals_same_st), field
	gsort -sh_deals_same_st ultimate_platform
	br ind_laurenassigned ultimate_platform deals com_hqstate deals_same_st sh_deals_same_st rank_st ///
		if rank_st == 1
		pause
restore

* Moments of distribution by primary, specialty, or inpatient
preserve
	keep if deals >= 12
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
			
	bys ultimate_platform statefips: gen deals_same_st = _N
	bys ultimate_platform cz: gen deals_same_cz = _N
	bys ultimate_platform: gen total_deals = _N
	
	drop if cz == . | statefips == .
	bys ultimate_platform: egen deals_top_state = max(deals_same_st)
	bys ultimate_platform: egen deals_top_cz = max(deals_same_cz)
	
	bys ultimate_platform: ereplace platformid = mode(platformid)
	keep ultimate_platform platformid ind_laurenassigned total_deals deals_top_state deals_top_cz
	duplicates drop
	
	gen top_state_sh = deals_top_state / total_deals
		replace top_state_sh = int(top_state_sh*1000)/10
	gen top_cz_sh = deals_top_cz / total_deals
		replace top_cz_sh = int(top_cz_sh*1000)/10
	
	collapse (mean) top_state_sh_mean = top_state_sh top_cz_sh_mean = top_cz_sh deals_mean = total_deals ///
			 (sd) top_state_sh_sd = top_state_sh top_cz_sh_sd = top_cz_sh deals_sd = total_deals ///
			 (iqr) top_state_sh_iqr = top_state_sh top_cz_sh_iqr = top_cz_sh deals_iqr = total_deals ///
			 (count) platforms = platformid ///
		, by(ind_laurenassigned)
		
	order ind_laurenassigned platforms deals_mean deals_sd deals_iqr ///
			top_cz_sh_mean top_cz_sh_sd top_cz_sh_iqr ///
			top_state_sh_mean top_state_sh_sd top_state_sh_iqr
br if !inlist(ind_laurenassigned, "Preschool/Childcare", "Educational Consulting", ///
	"Trade & Vocational Schools", "Education Travel", "Property Management", "HVAC & Plumbing")
pause
restore


