
pause on



*maptile_install using "http://files.michaelstepner.com/geo_hrr.zip"


cd "$proj_dir"

* Append 2013 and 2021 PE-ownership data to compare

use "processed-data/pe_estabs_hc_byspecialty_2013_da.dta", clear
append using "processed-data/pe_estabs_hc_byspecialty_2021_da.dta"

reshape wide n_estabs tot_pe pe_share pf_maxsh pf_hhi, ///
	i(hrrnum hrrcity hrrstate specialist size) j(year)

gen hrrname = hrrcity + ", " + hrrstate

* --- Tables of Top Specialities-by-HRR by PE Ownership in 2021 --- *

order hrrnum hrrname size specialist pe_share* pf_maxsh* pf_hhi* n_estabs* tot_pe*
	format %9.1f pe_share2021
gsort -pe_share2021
br hrrnum hrrname size specialist pe_share2021 n_estabs2021 if n_estabs2021 >= 10

* --- Tables of Top Specialities-by-HRR by Change in PE Ownership 2013-2021 --- *
gen dpe_share = pe_share2021-pe_share2013
	format %9.1f dpe_share

gsort -dpe_share
br hrrnum hrrname size specialist dpe_share pe_share2021 n_estabs2021 if n_estabs2021 >= 10

* --- Maps of PE Ownership --- *
ren hrrnum hrr
cd "output"
levelsof specialist, local(specs)
foreach s of local specs {
	maptile pe_share2021 if size == "X-Small (<50)" & specialist == "`s'", ///
				geo(hrr) cutv(5 10 30 50) stateoutline(thin) fc(Blues) res(0.25) ///
				savegraph("allpe21-`s'.png") replace
	maptile pf_maxsh2021 if size == "X-Small (<50)" & specialist == "`s'", ///
				geo(hrr) cutv(5 10 30 50) stateoutline(thin) fc(Blues) res(0.25) ///
				savegraph("toppf21-`s'.png") replace
	maptile pf_hhi2021 if size == "X-Small (<50)" & specialist == "`s'", ///
				geo(hrr) cutv(50 100 1000 5000) stateoutline(thin) fc(Blues) res(0.25) ///
				savegraph("pfhhi21-`s'.png") replace
}


