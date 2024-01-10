



pause on
cap cd "$proj_dir"


use "processed-data/pitchbook_bycbsa.dta", clear

keep addonplatform companyname
duplicates drop
drop if addonplatform == ""
*duplicates tag companyname, gen(dup) // only 3 companies purchased by more than one named platform
*br if dup

tempfile deals
save `deals', replace

drop companyname
bys addonplatform: gen deals = _N
duplicates drop
	tempfile pf_deals
	save `pf_deals', replace
ren deals prev_deals

ren addonplatform companyname
merge 1:m companyname using `deals', keep(1 3)

preserve // designate platforms never acquired in an add-on (with a named platform) as "ultimate" platforms
	keep if _merge == 1 // never acquired
	drop addonplatform
	ren companyname addonplatform
	local N = _N + 9
	dis `N'
	* Add croswalk for platforms that merged (i.e. they both acqired each other)
		set obs `N'
		replace addonplatform = "ABRA Auto Body & Glass" if _n == _N
		replace addonplatform = "AVI-SPL" if _n == _N - 1
		replace addonplatform = "Aquamar" if _n == _N - 2
		replace addonplatform = "Ascent Aviation Services" if _n == _N - 3
		replace addonplatform = "Cameron Wire & Cable" if _n == _N - 4
		replace addonplatform = "Forming and Machining Industries" if _n == _N - 5
		replace addonplatform = "LifeSize" if _n == _N - 6
		replace addonplatform = "Marketron Broadcast Solutions" if _n == _N - 7
		replace addonplatform = "Platinum Dermatology Partners" if _n == _N - 8
	gen platformtier = "U"
	tempfile ult_pfs
	save `ult_pfs', replace
	
	keep addonplatform
	gen ultimate_platform = addonplatform
	tempfile tier_u
	save `tier_u', replace
restore

keep if _merge == 3 // platforms that got acquired at least once
merge m:1 addonplatform using `ult_pfs', keep(1 3) nogen

preserve // saving crosswalk between initial platform and ultimate platform for tier 1
	keep if platformtier == "U"
	ren addonplatform ultimate_platform
	ren companyname addonplatform
	keep addonplatform ultimate_platform
	
	tempfile tier_i1
	save `tier_i1', replace
restore

keep if platformtier == ""
preserve // save merge from first acquirer to second acquirer
	keep addonplatform
	duplicates drop
	ren addonplatform companyname
	duplicates drop
	merge 1:m companyname using `deals', keep(1 3)
	tempfile tier_i2_xwalk
	save `tier_i2_xwalk', replace
restore

ren companyname platform1
drop platformtier
ren addonplatform companyname
joinby companyname using `tier_i2_xwalk', unm(master) _merge(acq_2)
merge m:1 addonplatform using `ult_pfs', keep(1 3) nogen

preserve // saving crosswalk between initial platform and ultimate platform for tier 2
	keep if platformtier == "U"
	ren addonplatform ultimate_platform
	ren platform1 addonplatform
	keep addonplatform ultimate_platform
	
	tempfile tier_i2
	save `tier_i2', replace
restore

keep if platformtier == ""
ren companyname platform2 // saving crosswalk between initial platform and ultimate for tier 3
drop platformtier
ren addonplatform companyname
merge 1:m companyname using `deals', keep(1 3) nogen
merge 1:1 addonplatform using `ult_pfs', keep(1 3) nogen

ren addonplatform ultimate_platform
ren platform1 addonplatform
keep addonplatform ultimate_platform

tempfile tier_i3
save `tier_i3', replace


* --- Append to get full crosswalk between platforms and ultimate platforms --- *
use `tier_u', clear
append using `tier_i1'
append using `tier_i2'
append using `tier_i3'

duplicates drop
* Core Systems acquired first by SecureAuth, then by HelpSystems
	replace addonplatform = "SecureAuth" ///
		if addonplatform == "Core Security" & ultimate_platform == "SecureAuth"
	replace ultimate_platform = "HelpSystems" ///
		if addonplatform == "SecureAuth" & ultimate_platform == "SecureAuth"
duplicates tag addonplatform, gen(dup)
br if dup








