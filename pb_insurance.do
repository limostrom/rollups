

cap cd "$proj_dir"


* -------------- Breakdown of Platforms in Insurance ---------------------------
import delimited "processed-data/states.csv", clear varn(1)
	ren statename com_hqstate_province
	ren statefips statefips_fill
	tempfile states
	save `states', replace

use "processed-data/pitchbook_bycbsa.dta", clear
keep dealid dealdate companyid companyname addonplatform platformid ///
	primaryindustrygroup primaryindustrycode cbsacode statefips com_hqstate
drop if addonplatform == ""
duplicates drop

gen year = real(substr(dealdate, -4, .))

egen dealid_num = group(dealid)
bys platformid: egen modal_indgrp = mode(primaryindustrygroup)
bys platformid: gen deals = _N
	keep if deals >= 24 // 99th percentile

keep if modal_indgrp == "Insurance"
	

* By State
merge m:1 com_hqstate_province using `states', keep(1 3) nogen keepus(statefips_fill)
	replace statefips = statefips_fill if statefips == .

drop if statefips == .
collapse (last) deals (count) deals_same_st = dealid_num, ///
	by(platformid addonplatform statefips com_hqstate)
	
gen sh_deals_same_st = deals_same_st / deals
	replace sh_deals_same_st = int(sh_deals_same_st*1000)/10
bys platformid: egen rank_st = rank(sh_deals_same_st), field
gsort -sh_deals_same_st addonplatform
br addonplatform deals com_hqstate deals_same_st sh_deals_same_st rank_st if rank_st == 1

