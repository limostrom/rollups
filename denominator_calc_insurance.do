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
global pb "/Users/laurenmostrom/Dropbox/Research/PitchBook/UCHICAGO_20220911"
global proj_dir "/Users/laurenmostrom/Dropbox/Research/Rollups"

cap cd "$proj_dir"

* (1) Compose list of company and platform names in the "Insurance" Group from PB
import delimited "processed-data/states.csv", clear varn(1)
	ren statename com_hqstate_province
	ren statefips statefips_fill
	tempfile states
	save `states', replace

use "processed-data/pitchbook_bycz.dta", clear
	merge m:1 addonplatform using "processed-data/layered_rollups_pb.dta", nogen assert(3)
keep dealid dealdate companyid companyname addonplatform platformid ultimate_platform ///
	primaryindustrygroup primaryindustrycode cz statefips com_hqstate
drop if addonplatform == ""
merge m:1 com_hqstate using `states', keep(1 3) nogen
	replace statefips = statefips_fill if statefips == .
	drop statefips_fill
duplicates drop

gen year = real(substr(dealdate, -4, .))

// Keep only companies whose industry group is Insurance
keep if primaryindustrygroup == "Insurance"
keep companyname addonplatform ultimate_platform statefips

// Standardize names of portfolio companies and platforms
stnd_compname companyname, ///
	gen(stn_companyname stn_dbaname stn_fkaname entitytype attn_name) ///
	patpath("/Users/laurenmostrom/Documents/Stata/ado")
keep stn_companyname statefips addonplatform ultimate_platform
stnd_compname addonplatform, ///
	gen(stn_platform stn_dbaname stn_fkaname entitytype attn_name) ///
	patpath("/Users/laurenmostrom/Documents/Stata/ado")
keep stn_companyname statefips stn_platform ultimate_platform
stnd_compname ultimate_platform, ///
	gen(stn_ultplatform stn_dbaname stn_fkaname entitytype attn_name) ///
	patpath("/Users/laurenmostrom/Documents/Stata/ado")
keep stn_companyname statefips stn_platform stn_ultplatform
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
order stn_companyname statefips stn_platform stn_ultplatform
duplicates drop
bys stn_companyname: egen modal_state = mode(statefips), minmode
	replace statefips = modal_state if statefips == .
	replace statefips = 0 if statefips == .
duplicates drop
isid stn_companyname statefips
tempfile ins_names
save `ins_names', replace



* (2) Standardize company names in Data Axle and merge the PB ones in
use company state abi parentnumber naics8 modeledemployeesize employeesize5location locationemployeesizecode ///
	if naics8 == "INSURANCE AGENCIES & BROKERAGES" using "$axle/2021.dta", clear

stnd_compname company, ///
	gen(stn_companyname stn_dbaname stn_fkaname entitytype attn_name) ///
	patpath("/Users/laurenmostrom/Documents/Stata/ado")

merge m:1 state using `states', keep(1 3) nogen
	rename statefips_fill statefips
merge m:1 stn_companyname statefips using `ins_names', keep(1 3)

gen size = "Small" if inlist(locationemployeesizecode, "A", "B", "C", "D", "E")
	replace size = "Medium" if inlist(locationemployeesizecode, "F", "G")
	replace size = "Large" if inlist(locationemployeesizecode, "H", "I", "J", "K")
	
gen pe = _merge == 3
collapse (count) n_estabs = abi (sum) pe, by(state statefips size)

gen pe_share = pe/n_estabs
	
/*
Allen Insurance Group
368938239

*/

