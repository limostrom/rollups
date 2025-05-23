library(httr)
library(jsonlite)
library(tidyverse)

# Set folder to save output to
outfolder <-"/Users/laurenmostrom/Dropbox/Research/Rollups/raw-data/"

# Set the API key and construct the base URL
api_key <- "a9849ca3e1f42d9273a95763547efb5db955c261"
base_url <- "https://api.census.gov/data/timeseries/bds"


# --- Pull firm counts for all MSAs --- #
# Define years and variables for query
years <- c(1995:2021)
variables <- c("NAME", "NAICS_LABEL", "YEAR", "FIRM", "ESTAB", "EMP")

for (y in years) {
    # Construct the full URL with parameters
    url <- paste0(base_url,
        "?get=", paste(variables, collapse = ","),
        "&for=state:*",
        "&time=", y,
        "&NAICS=5242",
        "&key=", api_key
    )
    # print url
    print(url)

    # Send the HTTP GET request and parse the response
    response <- GET(url)
    print(content(response))

    d <- fromJSON(content(response, "text"))
    # Remove first row
    d <- d[-1,]
    d <- as.data.frame(d)

    # Append other years and rename columns
    if (y == 1995) {
        d_all <- d
    } else {
        d_all <- rbind(d_all, d)
    }

    Sys.sleep(1)
}

d_all <- d_all %>%
    rename(cbsaname = V1, naics2 = V9, naics2short = V2,
        year = V3, firm = V4, estab = V5, emp = V6,
        cbsacode = V10) %>%
    select(-V8)

# Write to CSV
write.csv(d_all, paste0(outfolder, "bds_ins.csv"))
