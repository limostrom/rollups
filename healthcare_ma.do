
global pb_dir "/Users/laurenmostrom/Dropbox/Research/PitchBook/UCHICAGO_20220911"


cap cd "$pb_dir"


// Deal-Investor Relation
import delimited "DealInvestorRelation.csv", clear varn(1)
keep dealid investorid investorname investorstatus
	ren investorname acquirer
keep if investorstatus == "Acquirer"
tempfile dealinv
save `dealinv', replace


// Company info
import delimited "Company.csv", clear varn(1)
keep companyid companyname businessstatus hqlocation hqaddressline1 ///
	hqaddressline2 hqcity hqstate_province hqpostcode primaryindustry*
ren hq* com_hq*
tempfile companies
save `companies', replace


// Deals
import delimited "Deal.csv", clear varn(1)
keep companyid companyname dealid dealdate dealstatus dealtype
keep if dealstatus == "Completed"
keep if dealtype == "Merger/Acquisition"

merge m:1 companyid using `companies', nogen keep(1 3)
keep if primaryindustrygroup == "Healthcare Services"

merge 1:m dealid using `dealinv', nogen keep(1 3)

keep companyname acquirer dealdate dealid companyid primaryindustrygroup


stnd_compname companyname, ///
		gen(stn_companyname stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
	drop stn_dbaname stn_fkaname entitytype attn_name
	
stnd_compname acquirer, ///
		gen(stn_acquirer stn_dbaname stn_fkaname entitytype attn_name) ///
		patpath("/Users/laurenmostrom/Documents/Stata/ado")
	drop stn_dbaname stn_fkaname entitytype attn_name
	
save "$proj_dir/processed-data/healthcare_MA.dta", replace
export delimited "$proj_dir/processed-data/healthcare_MA.csv", replace
