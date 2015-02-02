# Geo Scrubber Script
# 01.26.2015 C. Willis
# This script downloads geo-reference data from GBIF, BISON or BIEN3 for a list of species
# It than scrubs these data based on distribution data from USDA Plants Database at the state and province scale
# It also include Mexico, but at the country level

# Load Packages
library(maptools)
require(sp)
require(rgdal)
require(maps)
library(dismo)
library(devtools)
install_github('rbison','ropensci')
library(rbison)
library(RPostgreSQL)
library(doParallel)
drv <- dbDriver('PostgreSQL')

# Read State and Province Shape files
# North American shapefile with US States, Canadian Provinces and Mexico  from GADM gadm.org
na.map = readShapePoly('~/Google Drive/Research/general data/North America Shape Files/gadm/North America/NorthAmerica.shp')

# Add FIPS codes to Shapefiles
fips = read.delim('~/Google Drive/Research/general data/North America Shape Files/gadm/North America/fips.txt')
na.map$FIPS = fips$FIPS

# Upload USDA State/Province database
# Created using usda.py script (B. Frazone)
usda.db = read.csv('~/Google Drive/Research/general data/USDA Distribution Data/master_cat_list.csv')

# Upload taxa list
taxa = read.delim('~/Google Drive/Research/projects/HUCE Projects/Unique Scrubbed NA Taxa List 2014.txt')
taxa$AcceptedName = as.character(taxa$AcceptedName)
taxa$AcceptedGenus = as.character(taxa$AcceptedGenus)
taxa$AcceptedSpecies = as.character(taxa$AcceptedSpecies)

########################
########################
#    Geo-Scrub Loop    #
########################

# Loop to download geo-reference data and scrubs it against USDA Plant Database
# Database is BIEN3

# Set user name and password for BIEN3 Database
USER = "username"
PWD = "password"

# Setup cluster to parallelize loop
cl <- makeCluster(4)
registerDoParallel(cl)

foreach(i = 1:length(taxa$AcceptedName),.packages=c('maptools','sp','rgdal','maps','dismo','devtools','RPostgreSQL')) %dopar% {

  # Print loop iteration with species name and time)
  #print(c(i,taxa$AcceptedName[[i]],date()),quote=F)
  # downloads geo-reference
# GBIF
  #gbf.dat = try(gbif(taxa$AcceptedGenus[[i]],paste(taxa$AcceptedSpecies[[i]],"*",sep=""),geo=T,removeZeros=T,concept=T),silent=T)
# BISON
  # Turned off BISON download, as the data often overlaps with GBIF
  #bis.dat = try(bison(taxa$AcceptedName[[i]],count=20000),silent=T)
# BIEN 3
  # Establishes link to BIEN3 PostgreSQL database
  drv <- dbDriver('PostgreSQL')
  con <- dbConnect(drv, dbname='vegbien',host='vegbiendev.nceas.ucsb.edu',port=5432,user=USER,password=PWD)
  # Downloads datafrom BIEN3
  # Note: much of the BIEN data derives from GBIF
  # Best to use one or the other
  bien.dat <- try(dbGetQuery(con, paste('SELECT * FROM view_full_occurrence_individual WHERE scrubbed_species_binomial = ','\'',taxa$AcceptedName[[i]],'\'',sep='')),silent=T)
  dbDisconnect(con) #closes psql connection so as not to hit limit of connections
  #Save raw BIEN3 data by species
  write.table(bien.dat,file=paste(taxa$AcceptedGenus[[i]],'_',taxa$AcceptedSpecies[[i]],'.bien3.raw.txt',sep=''))

# If there is not data for the species skip following steps

if(length(bien.dat) == 0) {

  write.table(cbind(taxa$AcceptedName[[i]],'no data',0,date()),file='taxa.ouput.txt',row.names=FALSE,col.names=FALSE,quote=FALSE,append=TRUE,sep=',')

  } else {

  # Reformat GBIF and BISON data and combined datasets
  #gbf.edit = as.data.frame(cbind(gbf.dat$species,gbf.dat$lon,gbf.dat$lat,paste(gbf.dat$institution,gbf.dat$collection,sep="-"),gbf.dat$catalogNumber,'gbif'))
  #names(gbf.edit) = c('taxa','lon','lat','original.source','ID','database')
  #bis.edit = as.data.frame(cbind(bis.dat$points$name,bis.dat$points$decimalLongitude,bis.dat$points$decimalLatitude,bis.dat$points$provider,bis.dat$points$occurrenceID,'bison'))
  #names(bis.edit) = c('taxa','lon','lat','original.source','ID','database')
  bien.edit = subset(bien.dat,!is.na(longitude)&!is.na(latitude),c(scrubbed_taxon_name_no_author,longitude,latitude))
  names(bien.edit) = c('taxa','lon','lat')

  # Possible option to combined GBIF and BISON data
  #geo.dat = rbind(gbf.edit,bis.edit)

  # Identify States / Provinces species is known to occur based on USDA Plants Database
  usda.list = subset(usda.db,taxa.name==taxa$AcceptedName[[i]])
  usda.list = unique(usda.list$fips)

  # Remove fips code from gbif data that are not NA shapefile
  sub.list = subset(usda.list, usda.list %in% na.map$FIPS)
  sub.list = as.character(sub.list)
  # Adds Mexico to list
  sub.list = c(sub.list,'MX00')
  # Create map with
  sub.map = na.map[which(na.map$FIPS %in% sub.list),]

  # Subset points by sub.map for BIEN3 data
  lonlat = as.data.frame(cbind(as.numeric(as.character(bien.edit$lon)),as.numeric(as.character(bien.edit$lat))))
  names(lonlat) = c('longitude','latitude')
  lonlat = subset(lonlat,!is.na(longitude)&!is.na(latitude))

  # If there is not geo-referenced data for the species, skip the remaining steps

  if(length(lonlat$longitude)==0) {

  write.table(cbind(taxa$AcceptedName[[i]],'no data',0,date()),file='taxa.ouput.txt',row.names=FALSE,col.names=FALSE,quote=FALSE,append=TRUE,sep=',')

    } else {

  coordinates(lonlat) = c('longitude','latitude')
  # Tell R that bear coordinates are in the same lat/lon reference system
  proj4string(lonlat) = proj4string(sub.map)

  # Combine is.na() with over() to do the containment test; note that we
  # Need to "demote" parks to a SpatialPolygons object first
  overlap = !is.na(over(lonlat, as(sub.map, 'SpatialPolygons')))

  # If there is no overlap between geo-referenced data and USDA skip remaining steps

  if(length(overlap[overlap==T]) == 0) {
    write.table(cbind(taxa$AcceptedName[[i]],'no data',0,date()),file='taxa.ouput.txt',row.names=FALSE,col.names=FALSE,quote=FALSE,append=TRUE,sep=',')
  } else {

  # If the geo-referenced data does overlap with USDA data write shapefile
    filtered.points = lonlat[overlap, ]
    df = as.data.frame(filtered.points)
    fp = SpatialPointsDataFrame(filtered.points,df)

    write.table(cbind(taxa$AcceptedName[[i]],'data',length(filtered.points),date()),file='taxa.ouput.txt',row.names=FALSE,col.names=FALSE,quote=FALSE,append=TRUE,sep=',')
    # Write filtered shapefile
    writePointsShape(fp, paste(taxa$AcceptedGenus[[i]],'_',taxa$AcceptedSpecies[[i]],'.bien3',sep=''))
      }
    }
  }
}
