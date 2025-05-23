/*
callstatuscode: "C" or "W" indicates employee size was verified
modeledemployeesize: "A" means actual, not modeled
employeesizelocation = number of employees (could be modeled, check above variables for confirmation)
locationemployeesizecode:
A	 1-4
B	 5-9
C	 10-19
D	 20-49
E	 50-99
F	 100-249
G	 250-499
H	 500-999
I	 1,000-4,999
J	 5,000-9,999
K	 10,000+
" "	BLANK
*/
pause on

global axle "/Volumes/Seagate Por/infogroup in Dropbox DevBanks/_original_data/"
global db "/Volumes/Seagate Por/D&B/"
global pb "/Users/laurenmostrom/Dropbox/Research/PitchBook/UCHICAGO_20220911"
global proj_dir "/Users/laurenmostrom/Dropbox/Research/Rollups"
global do_dir "/Users/laurenmostrom/Documents/GitHub/rollups"

cap cd "$proj_dir"


/* Silenced because run and saved
foreach y in 2013 2021 {
	use "processed-data/pitchbook_byhrr.dta", clear
		merge m:1 addonplatform using "processed-data/layered_rollups_pb.dta", nogen assert(3)
	keep dealid year companyid companyname addonplatform platformid ultimate_platform ///
		dealsynopsis primaryindustrygroup primaryindustrycode hrr* zipcode
	drop if addonplatform == ""
	duplicates drop
	
	keep if year <= `y'
	
	*gen lparen_pos = strpos(companyname, "(") if strpos(companyname, "(") > 0
	*br
	*pause 
	/* Multi-Establishment Acquisitions
	
	Brazos Valley Pathology (2 Hospital Units) acquired by Aurora Diagnostics, 2015
	Diagnostic Health (7 Outpatient Imaging Centers), 2011
	Radiologic Associates (Three Imaging Centers & Three Service Systems), Indiana
		acquired by Alliance HealthCare Services, 2002
		
	Therapeutics Unlimited (3 Clinics)
	ATI Physical Therapy (Three Clinics in California), 2021
	Therapeutics Unlimited (3 Clinics), 2018
	Garden State Orthopaedic Associates (2 Clinics, Fair Lawn and Clifton), 2018
	
	Davita (70 dialysis clinics), 2005
	Fresenius Medical Care (54 Clinics), 2012
	*/

	// Keep only companies whose industry group is Healthcare
	keep if primaryindustrygroup == "Healthcare Services"
	keep companyname year addonplatform ultimate_platform hrr* zipcode

	// Standardize names of portfolio companies and platforms
	stnd_compname companyname, ///
		gen(stn_companyname stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
	keep stn_companyname year hrrnum hrrcity hrrstate zipcode addonplatform ultimate_platform
	stnd_compname addonplatform, ///
		gen(stn_platform stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
	keep stn_companyname year hrrnum hrrcity hrrstate zipcode  stn_platform ultimate_platform
	stnd_compname ultimate_platform, ///
		gen(stn_ultplatform stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
	keep stn_companyname year hrrnum hrrcity hrrstate zipcode stn_platform stn_ultplatform
	duplicates drop

	// Keep Company List
	tempfile companies
	save `companies', replace

	// Keep platform names
	drop stn_companyname 
	ren stn_platform stn_companyname
	gen stn_platform = stn_ultplatform
	duplicates drop
	tempfile platforms
	save `platforms', replace

	// Keep names of ultimate platforms
	drop stn_companyname
	gen stn_companyname = stn_ultplatform
	append using `companies'
	append using `platforms'
	order stn_companyname year zipcode hrrnum stn_platform stn_ultplatform
	duplicates drop
	/*bys stn_companyname: egen modal_zip = mode(zipcode), minmode
		replace zipcode = modal_zip if zipcode == .
		replace zipcode = 0 if zipcode == .*/
	duplicates tag stn_companyname zipcode, gen(dup)
		drop if hrrnum == . & dup
		drop if zipcode == .
	drop stn_platform year dup
	duplicates drop
	isid stn_companyname zipcode
	gen com_zip_id = _n

	save "processed-data/hc_comnames`y'.dta", replace
}
*/

* Silenced because saved and takes forever to run
* (2) Standardize company names in D&B and merge the PB ones in

foreach y in /*2013*/ 2021 {
	
	import delimited "$db/DMI.`y'.CSV", clear
		
	keep if nationalcode == 0 // US, drop Canada
	
	keep companyname dunsno secondaryname nationalcode streetaddress city state zipcode businessdescription employeesthissite employeesthissitecode
	order companyname dunsno secondaryname nationalcode streetaddress city state zipcode businessdescription employeesthissite employeesthissitecode
	
	ren businessdescription busdesc
	
	#delimit ;
	keep if inlist(busdesc, "GNL MDL SRGL HSPTL", "HLTH ALLD SVCS NE",
		"HLTH ALLD SVCS NEC", "HOME HLTH CARE SVCS", "HSPL SPTY EX PSYTRC", "HSPTL MDCL SVC PLN",
		"INDVDL FAMILY SVCS", "KIDNEY DIALYSIS CTR", "MEDICL LABORATORIES")
		| inlist(busdesc, "OFCS CLNCS PODTRST", "OFCS CLNS CHRPRCTR", "OFCS CLNS CHRPRCTRS",
		"OFCS CLNS OF MDL D", "OFCS CLNS OF MDL DR", "OFCS HLTH PRNRS NE", "OFCS HLTH PRNRS NEC",
		"OFFCS OSTPATHC PHYS")
		| (busdesc == "UNDETERMINED"
			& (strpos(company, "MD PA") > 0
			| strpos(company, "MD PC") > 0
			| strpos(company, "MD PHD") > 0
			| strpos(company, "MD PLC") > 0
			| strpos(company, "MD PRO CORP") > 0
			| strpos(company, "MD INC") > 0
			| strpos(company, "MD SC") > 0
			| strpos(company, "MD MRCP") > 0
			| strpos(company, "MD FACS") > 0
			| strpos(company, "MD FRCS") > 0)) /* doctors' offices*/;
		
	#delimit cr

	gen year = `y'
		
	stnd_compname company, ///
		gen(stn_companyname stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
	stnd_compname secondaryname, ///
		gen(stn_2ndname stn_2nddbaname stn_2ndfkaname entitytype2nd attn_name2nd) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
		drop stn_2nddbaname stn_2ndfkaname entitytype2nd attn_name2nd
		
	save "processed-data/db_hc_stnnames`y'.dta", replace
}
*/

* (3) Merge PitchBook names into DA
foreach y in 2021 /*2013*/ {
	use "processed-data/db_hc_stnnames`y'.dta", clear

	// Dialysis
	*gen specialist = "Dialysis" if busdesc == "KIDNEY DIALYSIS CENTERS" | strpos(stn_companyname, "DIALYSIS") > 0 | strpos(stn_companyname, "RENAL") > 0

	// Physical Therapy
	#delimit ;
	gen specialist = "Physical Therapy" if ((strpos(stn_companyname, "PHYSICAL") + strpos(stn_companyname, "PHYSCL") + strpos(stn_companyname, "SPORT") > 0)
			& (strpos(stn_companyname, "THERAPY") + strpos(stn_companyname, "THRPY") + strpos(stn_companyname, "THRPSTS") + strpos(stn_companyname, "THERAPIST") > 0));
	replace specialist = "Physical Therapy" if busdesc == "OFFICES-PHYSICAL, OCCPTNL/SPEECH THRPSTS/AUDLGSTS"
		& strpos(stn_companyname, "PHYS") + strpos(stn_companyname, "SPORT") + strpos(stn_companyname, " PT") > 0;
	#delimit cr

	// Anesthesiologists
	replace specialist = "Anesthesiology" if strpos(stn_companyname, "ANESTHESI") > 0

	// Gastroenterologists
	replace specialist = "Gastroenterology" if strpos(stn_companyname, "GASTROENTEROLOG") ///
		+ strpos(stn_companyname, "DIGESTI") + strpos(stn_companyname, "GASTROINTESTIN") + strpos(stn_companyname, " GI ") > 0 ///
		| substr(stn_companyname, -3, .) == " GI"

	// Dermatologists
	replace specialist = "Dermatology" if strpos(stn_companyname, "DERMATOLOG") > 0 | strpos(stn_companyname, "SKIN") > 0

	// Podiatrists
	replace specialist = "Podiatry" if busdesc == "OFFICES OF PODIATRISTS" ///
		| strpos(stn_companyname, "PODIATR") + strpos(stn_companyname, "FOOT") > 0
		
	// Diagnostic Imaging
	replace specialist = "Diagnostic Imaging" if busdesc == "DIAGNOSTIC IMAGING CENTERS" ///
		| strpos(stn_companyname, "IMAGING") + strpos(stn_companyname, "MRI") > 0 ///
		| strpos(stn_companyname, "RADIOLOG") + strpos(stn_companyname, "RDLGY") > 0

	// OB/GYN
	replace specialist = "OB/GYN" if strpos(stn_companyname, "OBSTET") > 0 | strpos(stn_companyname, "GYNECOLOG") > 0 ///
		| strpos(stn_companyname, "OB/GYN") + strpos(stn_companyname, "OB-GYN") + strpos(stn_companyname, "OB GYN") > 0 ///
		| strpos(stn_companyname, "WOMEN'S HEALTH") + strpos(stn_companyname, "WOMENS HEALTH") + strpos(stn_companyname, "WOMENS HLTH") > 0 ///
		| strpos(stn_companyname, "MAMMOG") > 0

	// Cancer
	replace specialist = "Cancer" if strpos(stn_companyname, "ONCOLOG") + strpos(stn_companyname, "CANCER") + strpos(stn_companyname, "CHEMO") > 0

	keep if specialist != "" // speed up fuzzy merge

	merge m:1 zipcode year using "processed-data/zipcode_hrr_xwalk.dta", keep(1 3) nogen

	isid dunsno

	reclink2 stn_companyname zipcode using "processed-data/hc_comnames`y'.dta", ///
		gen(mscore) idm(dunsno) idu(com_zip_id) required(zipcode) manytoone minscore(0.99)	
		duplicates drop dunsno stn_ultplatform, force // should just be 1
	
	/*
		ren stn_companyname stn_firstname
		ren stn_2ndname stn_companyname
	reclink2 stn_companyname zipcode using "processed-data/hc_comnames`y'.dta", ///
		gen(mscore2) idm(dunsno) idu(com_zip_id) required(zipcode) manytoone minscore(0.99)	
		ren _merge _m2
	*merge m:1 stn_companyname hrrnum using "processed-data/hc_comnames.dta"
	br stn_companyname Ustn_companyname
	pause
	*/
	
	*include "$do_dir/fuzzy_merge_corrections.do"

	gen size = "X-Small (<50)" if inrange(employeesthissite, 0, 49)
		replace size = "Small (50-99)" if inrange(employeesthissite, 50, 99)
		replace size = "Medium (100-499)" if inrange(employeesthissite, 100, 499)
		replace size = "Large (>= 500)" if employeesthissite >= 500
		
	gen pe = _merge == 3
	
	preserve // --- Save list of names and firm addresses for competitors ------
		keep if size == "X-Small (<50)" & inlist(specialist, "Anesthesiology", "Gastroenterology", "Dermatology")
		keep if pe == 0
		keep stn_companyname specialist streetaddress city state zipcode
		order stn_companyname specialist streetaddress city state zipcode
		
		save "processed-data/nonpe_db.dta", replace
		export delimited "processed-data/nonpe_db.csv", replace
	restore // -----------------------------------------------------------------
	bys hrrnum size specialist: gen n_estabs = _N
	bys hrrnum size specialist: egen tot_pe = total(pe)
	bys hrrnum size specialist stn_ultplatform: egen platf_tot = total(pe)

	gen pe_share = tot_pe/n_estabs * 100
	gen platf_share = platf_tot/n_estabs * 100
		gen pfsh_sq = platf_share^2
		
	keep stn_ultplatform hrrnum hrrcity hrrstate specialist size n_estabs tot_pe pe_share platf_share pfsh_sq
	duplicates drop
	
	collapse (max) n_estabs tot_pe pe_share pf_maxsh = platf_share (sum) pf_hhi = pfsh_sq ///
		, by(hrrnum hrrcity hrrstate specialist size)
	
		lab var pe_share "Share of PE-Owned Establishments (%)"
		lab var pf_maxsh "Share of Establishments Owned by Largest Platform (%)"
		lab var pf_hhi "Platform HHI"
		lab var n_estabs "# of Establishments"
		lab var tot_pe "# of PE-Owned Establishments"
		
	gen year = `y'
	save "processed-data/pe_estabs_hc_byspecialty_`y'_db.dta", replace
		
	*gsort -pe_share specialist hrrnum size
	*br stn_companyname Ustn_companyname hrrcity size pe_share

	*keep tot_pe n_estabs pe_share specialist stn_ultplatform hrrnum hrrcity hrrstate
	*duplicates drop
}
