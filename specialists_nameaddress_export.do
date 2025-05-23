

use "processed-data/pitchbook_addons.dta", clear

replace companyname = strupper(companyname)
replace addonplatform = strupper(addonplatform)

keep if primaryindustrygroup == "Healthcare Services"

// Anesthesiologists
gen specialist = "Anesthesiology" if ///
	strpos(companyname, "ANESTH") > 0 | strpos(addonplatform, "ANESTH") > 0

// Gastroenterologists
replace specialist = "Gastroenterology" if ///
	strpos(companyname, "GASTROENTEROLOG") > 0 | strpos(companyname, "DIGESTI") > 0 ///
	| strpos(companyname, "GASTROINTESTIN") > 0 | strpos(companyname, " GI") > 0 ///
	| strpos(addonplatform, "GASTROENTEROLOG") > 0 | strpos(addonplatform, "DIGESTI") > 0 ///
	| strpos(addonplatform, "GASTROINTESTIN") > 0 | strpos(addonplatform, " GI") > 0

		
// Dermatology
replace specialist = "Dermatology" if ///
	strpos(companyname, "DERM") > 0 | strpos(addonplatform, "DERM") > 0 ///
	| strpos(companyname, "SKIN") > 0 | strpos(addonplatform, "SKIN") > 0

// Correct Mistakes
replace specialist = "" if inlist(addonplatform, "SIGHTMD", "AURORA DIAGNOSTICS", ///
	"PEOPLE, PETS & VETS", "PM PEDIATRICS", "NORTHEAST DENTAL MANAGEMENT")

keep if specialist != ""
keep dealdate companyname addonplatform specialist ///
	com_hqaddressline1 com_hqaddressline2 com_hqcity com_hqstate_province com_hqpostcode
order dealdate companyname addonplatform specialist ///
	com_hqaddressline1 com_hqaddressline2 com_hqcity com_hqstate_province com_hqpostcode
	
save "processed-data/pe_specialists.dta", replace
export delimited "processed-data/specialists_names_addresses.csv", replace
