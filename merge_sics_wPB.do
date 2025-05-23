


global proj_dir "/Users/laurenmostrom/Dropbox/Research/Rollups"
global pb_dir "/Users/laurenmostrom/Dropbox/Research/PitchBook/UCHICAGO_20220911"
global do_dir "/Users/laurenmostrom/Documents/GitHub/rollups"




cap cd "$pb_dir"

* --- Import Data Sets --- *
import delimited "Deal.csv", clear varn(1)
*keep companyid dealid dealdate dealstatus dealtype addon
keep if dealstatus == "Completed"
keep if dealtype == "Buyout/LBO"
	
tempfile deals
save `deals', replace


import delimited "Company.csv", clear varn(1)
keep companyid companyname primaryindustrygroup primaryindustrycode
	
tempfile companies
save `companies', replace

import delimited "CompanySicCodeRelation.csv", clear varn(1)
keep companyid siccode 
ren siccode sic

tempfile sics
save `sics', replace

import delimited "$proj_dir/processed-data/SIC_tradable.csv", clear varn(1)
tempfile markets
save `markets', replace

* --- Merge Together --- *
use `deals', clear
merge m:1 companyid using `companies', nogen keep(1 3)
joinby companyid using `sics', unm(none)

gen n_deals = 1
gen n_addons = (addon == "Yes")

bys sic: egen mode_primindgrp = mode(primaryindustrygroup), minmode
bys sic: egen mode_primindcode = mode(primaryindustrycode), minmode

collapse (sum) n_deals n_addons (last) primaryindustrygroup = mode_primindgrp primaryindustrycode = mode_primindcode, by(sic)
tempfile sicdeals
save `sicdeals', replace

use `markets', clear
merge m:1 sic using `sicdeals', nogen

replace n_deals = 0 if n_deals == .
replace n_addons = 0 if n_addons == .

lab var n_deals "# LBOS in PitchBook"
lab var n_addons "# Add-Ons in PitchBook"
lab var primaryindustrygroup "Modal Industry Group from PitchBook"
lab var primaryindustrycode "Modal Industry Code from PitchBook"

export delimited  "$proj_dir/processed-data/SIC_tradable_pbdeals.csv", replace
