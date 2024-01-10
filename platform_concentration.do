/*




*/





cap cd "$proj_dir"

* ------------------------ Breakdown of PitchBook Add-Ons ----------------------

use "processed-data/pitchbook_bycbsa.dta", clear


* By platform, how many purchased companies?
keep dealid dealdate companyid companyname addonplatform platformid primaryindustrygroup primaryindustrycode
drop if addonplatform == ""
duplicates drop

gen year = real(substr(dealdate, -4, .))

egen dealid_num = group(dealid)
bys platformid: egen modal_indgrp = mode(primaryindustrygroup)
gen is_modal_indgrp = primaryindustrygroup == modal_indgrp
bys platformid: egen modal_indcode = mode(primaryindustrycode)
gen is_modal_indcode = primaryindustrycode == modal_indcode

collapse (count) deals = dealid_num (last) modal_indgrp modal_indcode ///
	(sum) is_modal_indgrp is_modal_indcode (min) minyr = year (max) maxyr = year ///
	, by(addonplatform platformid)
	
egen tot_deals = total(deals)
gsort -deals modal_indgrp addonplatform
gen deal_share = deals / tot_deals
gen cum_deal_share = deal_share if _n == 1
replace cum_deal_share = deal_share + cum_deal_share[_n-1] if _n > 1
gen sh_modal_ind = is_modal_indgrp / deals
gen year_range = string(minyr) + "-" + string(maxyr)

// For Table top20.tex
order addonplatform deals cum_deal_share year_range modal_indgrp sh_modal_ind modal_indcode minyr maxyr
br addonplatform deals cum_deal_share year_range modal_indgrp sh_modal_ind if _n <= 20

// For Table percentiles.tex
summ deals, d


* Industry Breakdown within top 1 percent largest platforms
keep if deals >= r(p99)

graph hbar (count) deals, over(modal_indgrp, sort(1) descending label(labsize(small))) ///
	yti("Number of Platforms" "(Among Top Percentile Largest Platforms)", size(small))
	
graph export "output/bar_industry_platforms_top1pct.pdf", replace as(pdf)


* -------------- Breakdown of Platforms Within Opaque Groupings ----------------
use "processed-data/pitchbook_bycbsa.dta", clear
keep dealid dealdate companyid companyname addonplatform platformid primaryindustrygroup primaryindustrycode
drop if addonplatform == ""
duplicates drop

gen year = real(substr(dealdate, -4, .))

egen dealid_num = group(dealid)
bys platformid: egen modal_indgrp = mode(primaryindustrygroup)
bys platformid: gen deals = _N
	keep if deals >= 24 // 99th percentile


// Next Commercial Services and Products
preserve
	keep if modal_indgrp == "Commercial Services" | modal_indgrp == "Commercial Products"
	
	#delimit ;
	gen ind_laurenassigned = "Security"
		if inlist(addonplatform, "ASG Security", "Allied Universal", "Convergint Technologies",
								"Universal Services of America");
	replace ind_laurenassigned = "Records Management"
		if inlist(addonplatform, "Access Information Protected", "ArchivesOne");
	replace ind_laurenassigned = "Marketing"
		if inlist(addonplatform, "Acosta", "Advantage Solutions");
	replace ind_laurenassigned = "Waste / Garbage Removal"
		if inlist(addonplatform, "Advanced Disposal", "Environmental 360 Solutions",
								"GFL Environmental", "Lakeshore Recycling Systems", "Waste Pro USA");
	replace ind_laurenassigned = "HVAC & Plumbing"
		if inlist(addonplatform, "American Residential Services", "Coolsys", "Heartland Home Services");
	replace ind_laurenassigned = "Commercial Cleaning & Chemicals"
		if inlist(addonplatform, "Aramsco", "Sweeping Corporation of America");
	replace ind_laurenassigned = "Manufacturing"
		if inlist(addonplatform, "Arch Global Precision", "BlackHawk Industrial Distribution",
								"MW Industries");
	replace ind_laurenassigned = "Financial Services"
		if inlist(addonplatform, "Ascensus");
	replace ind_laurenassigned = "Commercial Real Estate"
		if inlist(addonplatform, "Avison Young");
	replace ind_laurenassigned = "Landscaping"
		if inlist(addonplatform, "BrightView Landscapes");
	replace ind_laurenassigned = "Events"
		if inlist(addonplatform, "Classic Party Rentals", "United Site Services");
	replace ind_laurenassigned = "Hiring"
		if inlist(addonplatform, "DISA Global Solutions");
	replace ind_laurenassigned = "Pest Control"
		if inlist(addonplatform, "Environmental Pest Service");
	replace ind_laurenassigned = "Vending Machines"
		if inlist(addonplatform, "Five Star Food Service");
	replace ind_laurenassigned = "Media"
		if inlist(addonplatform, "Hanley Wood Media", "Internet Brands");
	replace ind_laurenassigned = "Packaging"
		if inlist(addonplatform, "Imperial Dade");
	replace ind_laurenassigned = "Maintenance of Manufacturing Facilities"
		if inlist(addonplatform, "Industrial Service Solutions");
	replace ind_laurenassigned = "Building Materials & Products"
		if inlist(addonplatform, "Installed Building Products", "Kodiak Building Partners",
								"SRS Distribution", "Summit Materials (Building Products)",
								"Tecta America", "The Cook & Boardman Group", "US LBM Holdings");
	replace ind_laurenassigned = "Consulting"
		if inlist(addonplatform, "J.S. Held");
	replace ind_laurenassigned = "Legal Services"
		if inlist(addonplatform, "Lexitas");
	replace ind_laurenassigned = "Cold Chain"
		if inlist(addonplatform, "Lineage Logistics Holdings");
	replace ind_laurenassigned = "Construction Equipment"
		if inlist(addonplatform, "Maxim Crane Works");
	replace ind_laurenassigned = "Environmental Services"
		if inlist(addonplatform, "Montrose Environmental Group");
		
		
		
	#delimit cr
	
	keep addonplatform deals ind_laurenassigned
	duplicates drop
	gsort ind_laurenassigned
	br
	
	
	graph hbar (sum) deals, over(ind_laurenassigned, sort(1) descending label(labsize(small))) ///
		yti("Number of Deals" "(Among Top Percentile Largest Healthcare Platforms)", size(small))
		
	graph export "output/bar_healthcare_platforms_top1pct.pdf", replace as(pdf)
restore
