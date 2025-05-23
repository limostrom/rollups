
/*
Just trying to see what's in the Pitchbook data



*/



global proj_dir "/Users/laurenmostrom/Dropbox/Research/Rollups"
global data_dir "/Users/laurenmostrom/Dropbox/Research/PitchBook/UCHICAGO_20220911"
global do_dir "/Users/laurenmostrom/Documents/GitHub/rollups"


cap cd "$data_dir"

* --- Import Data Sets --- *
import delimited "Deal.csv", clear varn(1)
keep companyid dealid dealdate dealstatus dealsynopsis dealtype dealtype2 dealtype3 ///
	dealclass addon addonsponsor addonplatform sitelocation
keep if addon == "Yes"
	
tempfile deals
save `deals', replace


import delimited "Company.csv", clear varn(1)
keep companyid companyname businessstatus hqlocation hqaddressline1 ///
	hqaddressline2 hqcity hqstate_province hqpostcode primaryindustry*
ren hq* com_hq*
	
tempfile companies
save `companies', replace

import delimited "DealInvestorRelation.csv", clear varn(1)
keep dealid investorid investorfundid investorfundname

tempfile deal_inv
save `deal_inv', replace

import delimited "Investor.csv", clear varn(1)
keep investorid investorname hqlocation hqaddressline1 hqaddressline2 ///
	 hqcity hqstate_province hqpostcode preferredinvestmenttypes
ren hq* inv_hq*
	 
tempfile investors
save `investors', replace

* --- Merge --- *

cap cd "$proj_dir"

use `deals', clear
merge m:1 companyid using `companies', nogen keep(1 3)
merge 1:m dealid using `deal_inv', nogen keep(1 3)
merge m:1 investorid using `investors', nogen keep(1 3)

keep if dealstatus == "Completed"

order dealid dealdate companyid investorid 

egen platformid = group(addonplatform)

gen pb_id = _n

gen zipcode = regexs(0) if(regexm(com_hqpostcode, "[0-9][0-9][0-9][0-9][0-9]"))
	destring zipcode, replace

stnd_compname companyname, ///
		gen(stn_companyname stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
	drop stn_dbaname stn_fkaname entitytype attn_name

save "processed-data/pitchbook_addons.dta", replace
export delimited "processed-data/pitchbook_addons.csv", replace

/*preserve // for geocoding these headuarters
	keep companyid com_hqaddressline1 com_hqaddressline2 com_hqcity com_hqstate com_hqpostcode
	gen com_hqaddress = com_hqaddressline1 + " " + com_hqaddressline2
	drop com_hqaddressline1 com_hqaddressline2
	order companyid com_hqaddress com_hqcity com_hqstate com_hqpostcode
	forval i = 1(10000)120001 {
	local j = `i' + 9999
		export delimited if _n >= `i' & _n <= `j' using ///
			"processed-data/company_addresses_`i'_`j'.csv", replace novarnames
	}
restore
*/
