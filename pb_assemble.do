


cap cd "$proj_dir"


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
	import delimited "../raw-data/cbsa2fipsxw.csv", clear
	keep cbsacode fipsstatecode fipscountycode
	ren fipsstatecode statefips
	ren fipscountycode countyfips
	duplicates drop
	tempfile countyXcbsa
	save `countyXcbsa', replace
restore

merge m:1 statefips countyfips using `countyXcbsa', keep(1 3) nogen

save "pitchbook_bycbsa.dta", replace

/*


egen dealid_num = group(dealid)
collapse (count) deals = dealid_num, by(addonplatform platformid)

hist deals, percent
hist deals if deals > 10, percent
*/


