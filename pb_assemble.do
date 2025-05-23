


cap cd "$proj_dir"

* -- Save CBSA Crosswalk --- *

	import delimited "raw-data/cbsa2fipsxw.csv", clear
	keep cbsacode fipsstatecode fipscountycode
	ren fipsstatecode statefips
	ren fipscountycode countyfips
	duplicates drop
	tempfile countyXcbsa
	save `countyXcbsa', replace
	
* --- Save CZ Crosswalk --- *

	import excel "raw-data/cz00_eqv_v1.xls", clear first
	gen statefips = real(substr(FIPS, 1, 2))
	gen countyfips = real(substr(FIPS, 3, 3))
	ren CommutingZoneID2000 cz
	keep statefips countyfips cz
	duplicates drop
	tempfile countyXcz
	save `countyXcz', replace

* --- Save HRR Crosswalk --- *

	local filelist: dir "raw-data" files "ZipHsaHrr??.csv"
	cd "raw-data"
	local ii 1
	
	foreach f of local filelist {
		import delimited "`f'", clear varn(1)
		isid zip
		ren zip* zipcode
		local y = substr("`f'", 10, 2)
		gen year = "20" + "`y'"
			destring year, replace
			
		if `ii' > 1 {
			append using `ziphrr'
			save `ziphrr', replace
		}
		
		if `ii' == 1 {
			tempfile ziphrr
			save `ziphrr', replace
			local ++ii
		}
	}
	
	expand 4 if year == 2019
	bys zipcode year: replace year = year - 1 + _n
	save `ziphrr', replace
	cd ../
	
	local filelist: dir "raw-data" files "ZipHsaHrr??.xls"
	cd "raw-data"
	local ii 1
	
	foreach f of local filelist {
		import excel "`f'", clear first
		isid zip
		ren zip* zipcode
		local y = substr("`f'", 10, 2)
		gen year = "20" + "`y'"
			destring year, replace
			
		append using `ziphrr'
		save `ziphrr', replace
	}
	cd ../
	save "processed-data/zipcode_hrr_xwalk.dta", replace

* --- Append all geocoded files together --- *

local filelist: dir "processed-data/" files "company_geocodes_*.csv" 

cd "processed-data"
local ii 1

foreach file of local filelist {
	import delimited "`file'", clear varn(nonames)
	
	if `ii' == 1 {
		tempfile geos
		save `geos', replace
		
		local ++ii
	}
	else {
		append using `geos'
		save `geos', replace
	}
}

ren (v1 v5 v6 v9 v10 v11) (companyid address coords statefips countyfips tract)
split coords, p(",")
ren coords1 lon
ren coords2 lat
destring lon lat, replace

keep companyid statefips countyfips tract lon lat
duplicates drop
duplicates tag companyid, gen(dup)
drop if dup & statefips == .
isid companyid
drop dup

tempfile geocodes
save `geocodes', replace


* --- Merge geocodings on Company ID --- *

use "pitchbook_addons.dta", clear

merge m:1 companyid using `geocodes', keep(1 3) nogen



export delimited "pb_addons_geo.csv", replace // export to bring into R


* --- Merge countyfips to CBSA codes -- *

preserve
	merge m:1 statefips countyfips using `countyXcbsa', keep(1 3) nogen

	save "pitchbook_bycbsa.dta", replace
restore

* --- Merge countyfips to Commuting Zone -- *

preserve
	merge m:1 statefips countyfips using `countyXcz', keep(1 3) nogen

	save "pitchbook_bycz.dta", replace
restore

* --- Merge zipcodes to Hospital Referral Regions --- *

preserve
	gen zipcode = regexs(0) if(regexm(com_hqpostcode, "[0-9][0-9][0-9][0-9][0-9]"))
	gen year = real(substr(dealdate, -4, .))
		destring zipcode year, replace
		
	merge m:1 zipcode year using `ziphrr', keep(1 3) nogen

	save "pitchbook_byhrr.dta", replace
restore


/*


egen dealid_num = group(dealid)
collapse (count) deals = dealid_num, by(addonplatform platformid)

hist deals, percent
hist deals if deals > 10, percent
*/


