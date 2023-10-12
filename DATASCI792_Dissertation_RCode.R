## Load the required libraries
library(tidyverse)
library(lubridate)
library(vroom)
library(geohashTools)
library(sf)
library(sp)
library(tmap)

## Appendix A.1 -- Flexible Geofencing

### Function 1
geofence <- function(geohash, width){
  gh <- geohash
  if (width > 0){
    for (i in 1:width){
      gh <- unique(unlist(gh_neighbors(gh)))
    }
  }
  gh
}

### Function 2
vgeofence <- function(geohash, width = 1){
  l = split(geohash, seq_along(geohash))
  result = bind_rows(lapply(l, geofence, width))
  as_tibble(result)
}

## Appendix A.2 -- Footfall Aggregation

### Function 1
footfall_one = function(file, ghl = 9){
  # read the files, filter out Auckland refion, get geohash of specified length
  df_var = suppressMessages(
    vroom(file, col_select = c(1,3,4,6,16), col_names = FALSE) |> 
      rename(ID = X1, Latitude = X3, Longitude = X4, DateTime = X6, GeoHash = X16) |>
      filter(Latitude >= -37.2 & Latitude <= -36.53 & 
               Longitude >= 174.44 & Longitude <= 175.14) |>
      mutate(DateTime = as.Date(DateTime),
             GeoHash = substr(GeoHash, 1, ghl)))
  
  # get date
  Date = unique(unlist(df_var$DateTime))
  
  # get the total count per geohash
  df_total = df_var |> 
    group_by(GeoHash) |> 
    summarise(total_cnt = n()) 
  
  # get the total count per device ID and geohash (i.e. unqie device count)
  df_unique = df_var |> 
    distinct(ID, GeoHash) |>
    group_by(GeoHash) |> 
    summarise(unique_cnt = n()) 
  
  # combine the above two results and return
  tibble(Date, suppressMessages(inner_join(df_total, df_unique)))
}

### Function 2
footfall_gf = function(df, width = 2){
  for (w in 1:width){
    # get all the geohashes in a geofence
    gf = vgeofence(df$GeoHash, width = w)
    
    # change the shape of the previous output into a long form
    t = pivot_longer(gf, cols = everything(), names_to = "Count", values_to = "GeoHash")
    
    # join the tibble of interest with previous input
    df_temp = suppressMessages(left_join(t, df))
    
    # geofencing
    df_w = df_temp |> 
      group_by(Count) |>
      mutate(Count = as.integer(Count)) |>
      summarise(gf_total_cnt = sum(total_cnt, na.rm = TRUE), 
                gf_unique_cnt = sum(unique_cnt, na.rm = TRUE)) |>
      rename_with(~paste0("width", w, "_", .x), starts_with("gf"))
    
    # combine the result of geofencing and original tibble
    df = suppressMessages(bind_cols(df, df_w[,-1]))
  }
  df
}

### Function 3
footfall_with_gf = function(year, month, ghl = 9, width = 2){
  # get the directory of the folders of data files
  directory = list.dirs(paste0("year=", year, "/month=", sprintf("%02s", month)))[-1]
  for (i in seq_along(directory)){
    start = Sys.time()
    
    # get the relative paths of the files in the directory
    file = list.files(directory[i])
    file = paste0(directory[i], "/", file)
    
    # read the data files, compute the footfall with geofencing
    result = footfall_one(file, ghl)
    result = footfall_gf(result, width)
    end = Sys.time()
    str_name = paste0(year, sprintf("%02s", month), sprintf("%02s", i))
    message(paste(str_name, "done! Time used =", 
                  round(difftime(end, start, units = "secs"), 2), "seconds!"))
    
    # save the (daily) result in a csv file
    result |> write_csv(paste0("Footfall/", str_name, ".csv"))
  }
}

## Appendix A.3 -- Retail Aggregation

### Function 1
retail_df = function(yymmdd){
  ## obtain a list of the desired retail property types for further analysis
  is_retail = suppressMessages(
    read_csv("retail_property_types.csv",col_select = 1:2) |>
      filter(is_retail == 1) |>
      select(types) |>
      unlist())
  
  ## parse POI to a tibble
  poi = read_sf(dsn = paste0("new-zealand-", yymmdd, "-free"), 
                layer = "gis_osm_pois_free_1")
  coord = unlist(poi$geometry)
  num_rows = length(coord) %/% 2
  df_poi = tibble(name = poi$name,
                  type = poi$fclass,
                  lon = coord[seq(1, by = 2, length.out = num_rows)],
                  lat = coord[seq(2, by = 2, length.out = num_rows)])  |>
    filter(lat >= -37.2 & lat <= -36.53 & 
             lon >= 174.44 & lon <= 175.14) |>
    filter(type %in% is_retail) |>
    mutate(GeoHash = gh_encode(lat, lon, 9L))
  
  ## parse POI_area to a tibble
  poi_a = read_sf(dsn = paste0("new-zealand-", yymmdd, "-free"), 
                  layer = "gis_osm_pois_a_free_1")
  df_coord_poi_a = t(sapply(1:nrow(poi_a), 
                            function(i) colMeans(matrix(unlist(poi_a[i, "geometry"]), ncol=2))))
  df_poi_a = tibble(name = poi_a$name,
                    type = poi_a$fclass,
                    lon = df_coord_poi_a[,1],
                    lat = df_coord_poi_a[,2]) |>
    filter(lat >= -37.2 & lat <= -36.53 & 
             lon >= 174.44 & lon <= 175.14) |>
    filter(type %in% is_retail) |>
    mutate(GeoHash = gh_encode(lat, lon, 9L))
  
  ## merge two tibbles together
  as_tibble(rbind(df_poi, df_poi_a))
}

### Function 2
retail_gf = function(df, width = 2){
  # convert the original df such that GeoHash is unique
  temp = df |> group_by(GeoHash, type) |> 
    summarise(n = n()) |> 
    pivot_wider(names_from = type, names_prefix = "type_", 
                values_from = n, values_fill = 0)
  
  # similar to footfall_gf function, only different aggregation method
  for (w in 0:width){
    gf = vgf(df$GeoHash, width = w)
    t = pivot_longer(gf, cols = everything(), names_to = "Count", values_to = "GeoHash")
    df_temp = suppressMessages(left_join(t, temp))
    df_w = df_temp |> 
      group_by(Count) |>
      mutate(Count = as.integer(Count)) |>
      summarise(gf_total_cnt = sum(c_across(starts_with("type_")), na.rm = TRUE), 
                gf_unique_cnt = sum(colSums(across(starts_with("type_")), na.rm = TRUE) > 0, na.rm = TRUE)) |>
      rename_with(~paste0("width", w, "_", .x), starts_with("gf"))
    df = suppressMessages(bind_cols(df, df_w[,-1]))
  }
  df
}

### Function 3
retail_with_footfall = function(year, qty, df){
  for (m in 1:12){
    # read the daily footfall files
    file_m = paste0("Footfall", "/", list.files("Footfall"))
    footfall = suppressMessages(
      vroom(file_m[grepl(paste0(year, sprintf("%02s", m)), file_m)]))
    
    # convert it to a wide form
    footfall = footfall[, c("Date", "GeoHash", qty)] |> 
      pivot_wider(names_from = Date, values_from = qty)
    
    # merge with the target tibble
    df = suppressMessages(df |> left_join(footfall))
  }
  df
}

## Appendix A.4 -- Visualisation
convert_to_shp = function(df, convert = FALSE, filename = ""){
  gh = unique(unlist(df$GeoHash))
  
  # Convert to sf object with bounding boxes of geohashes
  shp_obj = gh_to_sf(gh)
  
  # Merge with the original df
  shp_obj$GeoHash = rownames(shp_obj)
  shp_obj = suppressMessages(shp_obj |> left_join(df))
  
  # Export sf object as a .shp file
  if (convert) st_write(shp_obj, filename)
  shp_obj
}

