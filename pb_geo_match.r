# Fill in missing Lon/Lat coordinates as centroid of company zipcode

library(dplyr)
library(sf)

#Change working directory
setwd("/Users/laurenmostrom/Dropbox/Personal Document Backup/Booth/Third Year/Rollups/")

# Read in Pitchbook data
pb <- read.csv("processed-data/pb_addons_geo.csv", header = TRUE, stringsAsFactors = FALSE)

# Pull out companies with coordinates, use 4269 for NAD83
pb_coords <- pb %>%
  filter(!is.na(lon))%>%
  st_as_sf(coords = c("lon", "lat"), crs = 4269) %>%
  mutate(centroid_flag = 0)

# Pull out companies with zipcodes but no coordinates
pb_zips <- pb %>%
  mutate(com_hqpostcode = ifelse(grepl("[A-Za-z]", com_hqpostcode), NA, as.numeric(com_hqpostcode))) %>%
  filter(is.na(lon) & !is.na(com_hqpostcode))%>%
  select(-lon, -lat) %>%
  mutate(centroid_flag = 1)

# Read in ZCTA boundaries
zcta <- st_read("raw-data/tl_2020_us_zcta520/tl_2020_us_zcta520.shp")
zcta <- zcta %>%
  mutate(coords = st_centroid(geometry),
         lon = st_coordinates(coords)[,1], lat = st_coordinates(coords)[,2],
         ZCTA5CE20 = as.numeric(ZCTA5CE20))

# Merge in centroid coordinates using zipcode
pb_zips <- left_join(pb_zips, zcta %>% select(ZCTA5CE20, lon, lat),
    by = c("com_hqpostcode" = "ZCTA5CE20"))

# Set to EPSG 4326, so we can append this to pb_coords
pb_zips <- pb_zips %>%
  filter(!is.na(lon))
pb_zips <- st_as_sf(pb_zips, coords = c("lon", "lat"), crs = 4269)

pb <- rbind(pb_coords, pb_zips)

# Read in CBSA polygons
cbsa <- st_read("raw-data/tl_2020_us_cbsa/tl_2020_us_cbsa.shp")

# Merge pb with the CBSA polygons
merged <- st_join(cbsa, pb, join = st_contains)


# Filter the first deal for each platformid
    # Convert dealdate to date variable
merged$dealdate <- as.Date(merged$dealdate, format = "%m/%d/%Y")

write.csv(merged, "processed-data/pb_addons_geo_cbsa.csv", row.names = FALSE)

first_deals <- merged %>%
  group_by(platformid) %>%
  filter(dealdate == min(dealdate)) %>%
  ungroup() %>%
  as.data.frame()

# Compute the distance between each point and the earliest row's point
distances <- merged %>%
  left_join(first_deals, by = "platformid") %>%
  mutate(distance = st_distance(geometry.x, geometry.y))

# distances data frame will contain the distances between each point and the earliest row's point for each platformid