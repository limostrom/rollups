
cap mkdir "$axle/healthcare"

forval y = 2013/2021 {
	use company zipcode abi parentnumber primarynaics naics8 ///
		addressline1 city state zipcode ///
		modeledemployeesize employeesize5location locationemployeesizecode ///
		using  "$axle/`y'.dta", clear
		
	#delimit ;
	keep if inlist(naics8, "ALL OTHER MISC AMBULATORY HEALTH CARE SERVICES", "ALL OTHER OUTPATIENT CARE CENTERS",
		"AMBULANCE SERVICES", "BLOOD & ORGAN BANKS", "DIAGNOSTIC IMAGING CENTERS",
		"DIRECT HEALTH & MEDICAL INSURANCE", "FREESTANDING AMBULATORY SURGICAL & EMERGENCY CTRS",
		"GENERAL MEDICAL & SURGICAL HOSPITALS") | inlist(naics8, "HMO MEDICAL CENTERS", "HOME HEALTH CARE SERVICES",
		"KIDNEY DIALYSIS CENTERS", "MEDICAL, DENTAL/HOSPITAL EQUIP/SUPLS MRCHNT WHLSRS", "OFFICES OF CHIROPRACTORS",
		"OFFICES OF MENTAL HEALTH PHYSICIANS", "OFFICES OF PHYSICIANS (EXC MENTAL HEALTH SPECS)",
		"OFFICES OF PODIATRISTS", "OFFICES-MENTAL HEALTH PRACTITIONERS")
		| inlist(naics8, "OFFICES-PHYSICAL, OCCPTNL/SPEECH THRPSTS/AUDLGSTS", "OFFICES OF ALL OTHER MISC HEALTH PRACTITIONERS",
		"OUTPATIENT MENTAL HEALTH & SUBSTANCE ABUSE CTRS", "PSYCHIATRIC & SUBSTANCE ABUSE HOSPITALS",
		"SPECIALTY (EXC PSYCHIATRIC/SUBSTANCE ABUSE) HSPTL", "VOCATIONAL REHABILITATION SERVICES")
		| (naics8 == "UNCLASSIFIED ESTABLISHMENTS"
			& (strpos(company, "MD PA") > 0
			| strpos(company, "MD PC") > 0
			| strpos(company, "MD PHD") > 0
			| strpos(company, "MD PLC") > 0
			| strpos(company, "MD PRO CORP") > 0
			| strpos(company, "MD INC") > 0
			| strpos(company, "MD SC") > 0
			| strpos(company, "MD MRCP") > 0
			| strpos(company, "MD FACS") > 0
			| strpos(company, "MD FRCS") > 0));
	#delimit cr

	gen year = `y'
	/*
	// Anesthesiologists
	gen specialist = "Anesthesiology" if strpos(company, "ANESTHESI") > 0

	// Gastroenterologists
	replace specialist = "Gastroenterology" if strpos(company, "GASTROENTEROLOG") ///
		+ strpos(company, "DIGESTI") + strpos(company, "GASTROINTESTIN") + strpos(company, " GI ") > 0 ///
		| substr(company, -3, .) == " GI"

	// Dermatologists
	replace specialist = "Dermatology" if strpos(company, "DERMATOLOG") > 0 | strpos(company, "SKIN") > 0

	keep if specialist != ""
		*/
		
	stnd_compname company, ///
		gen(stn_companyname stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
		drop  stn_dbaname stn_fkaname entitytype attn_name
		
	save "$axle/healthcare/axle_healthcare_stnnames`y'.dta", replace
	
}

forval y = 2014/2021 {
	use "processed-data/axle_hc_stnnames`y'.dta", clear
	
	gen da_id = _n

	reclink2 stn_companyname zipcode using "processed-data/pitchbook_addons.dta", ///
			gen(mscore) idm(da_id) idu(pb_id) required(zipcode) manytoone minscore(0.99)
	
	keep company stn_companyname abi Ustn_companyname year parentnumber addonplatform dealid ///
		companyid dealdate addressline1 city state zipcode
	order company stn_companyname abi Ustn_companyname year parentnumber addonplatform dealid ///
		companyid dealdate addressline1 city state zipcode
	
	ren Ustn_companyname pitchbook_name
	gen pe = (pitchbook_name != "")

	gen dealyear = substr(dealdate, -4, .)
		destring dealyear, replace
	replace pe = 0 if pe == 1 & dealyear > year
	
	compress *
	save "$axle/healthcare/axle_pb_merged`y'.dta", replace
}

use "$axle/healthcare/axle_pb_merged2013.dta", clear

forval y = 2014/2021 {
	append using "$axle/healthcare/axle_pb_merged`y'.dta"
}

isid abi year
save "axle_panel.dta", replace
export delimited "processed-data/axle_panel.csv", replace
