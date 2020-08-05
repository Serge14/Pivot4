library(data.table)
library(stringi)

source("functions.R")
source("dictionaries.R")

df.matrix = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.matrix.csv")

### Step 1. Proxima - Baby Food

# Step 1.1. Read raw data
df = fread("/home/sergiy/Documents/Work/Nutricia/Data/202006/P_2020_06.csv",
           check.names = TRUE)
df.sku.proxima = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.proxima.csv")
df.p  = fread("/home/sergiy/Documents/Work/Nutricia/Data/BF_PH_2015-2020M05.csv")

# Save a copy of existig descriptions
fwrite(df.sku.proxima, 
       paste0("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.proxima-", 
              Sys.Date(), ".csv"), 
       row.names = FALSE)

# Step 1.2. Define new descriptions
df.new = export.new.skus(df, df.sku.proxima)

# Step 1.3. Append new descriptions to existing ones

# Just in case the data contain Size.pharma that is not used
# It is excluded by the following line
cols = names(df.new)[names(df.new) %in% names(df.sku.proxima)]

df.sku.proxima = rbindlist(list(df.sku.proxima, df.new[, mget(cols)]), 
                           fill = TRUE)
similar.description(df.sku.proxima)

# Step 1.4. Save updated file of descriptions
fwrite(df.sku.proxima,
       "/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.proxima.csv",
       row.names = FALSE)

# Step 1.5. Update sales file
df = raw.data.processing(df)
df.p = append.new.data(df, df.p, 0.25)
check.historical.data(df.p)

fwrite(df.p, "/home/sergiy/Documents/Work/Nutricia/Data/BF_PH_2015-2020M06.csv",
       row.names = FALSE)


### Step 2. Proxima - AMN

# Step 2.1. Read raw data
df = fread("/home/sergiy/Documents/Work/Nutricia/Data/202006/AMN_2020_06.csv",
           check.names = TRUE)
df.sku.proxima = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.proxima.csv")
df.amn  = fread("/home/sergiy/Documents/Work/Nutricia/Data/AMN_PH_2015-2020M05.csv")

# Save a copy of existig descriptions
fwrite(df.sku.proxima, 
       paste0("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.proxima-", 
              Sys.Date(), ".csv"), 
       row.names = FALSE)

# Step 2.2. Define new descriptions
df.new = export.new.skus(df, df.sku.proxima)

# Step 2.3. Append new descriptions to existing ones

# Just in case the data contain Size.pharma that is not used
# It is excluded by the following line
cols = names(df.new)[names(df.new) %in% names(df.sku.proxima)]

df.sku.proxima = rbindlist(list(df.sku.proxima, df.new[, mget(cols)]), 
                           fill = TRUE)
similar.description(df.sku.proxima)

# Step 2.4. Save updated file of descriptions
fwrite(df.sku.proxima,
       "/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.proxima.csv",
       row.names = FALSE)

# Step 2.5. Update sales file
df = raw.data.processing(df)
df.amn = append.new.data(df, df.amn, 0.50)
check.historical.data(df.amn)

fwrite(df.amn, "/home/sergiy/Documents/Work/Nutricia/Data/AMN_PH_2015-2020M06.csv",
       row.names = FALSE)

### Step 3 Nielsen

# Step 3.1. Read raw data
df = fread("/home/sergiy/Documents/Work/Nutricia/Data/202006/N_2020_06.csv",
           check.names = TRUE)
df.sku.nielsen = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.nielsen.csv")
df.n  = fread("/home/sergiy/Documents/Work/Nutricia/Data/N_MT_2015-2020M05.csv")

# Save a copy of existig descriptions
fwrite(df.sku.nielsen, 
       paste0("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.nielsen-", 
              Sys.Date(), ".csv"), 
       row.names = FALSE)

# Step 3.2. Define new descriptions
df.new = export.new.skus(df, df.sku.nielsen)

# Step 3.3. Append new descriptions to existing ones

# Just in case the data contain Size.pharma that is not used
# It is excluded by the following line
cols = names(df.new)[names(df.new) %in% names(df.sku.nielsen)]

df.sku.nielsen = rbindlist(list(df.sku.nielsen, df.new[, mget(cols)]), 
                           fill = TRUE)

# Step 3.4. Save updated file of descriptions
fwrite(df.sku.nielsen,
       "/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.nielsen.csv",
       row.names = FALSE)

# Step 3.5. Update sales file
df = raw.data.processing(df)
df.n = append.new.data(df, df.n, 0.20)
check.historical.data(df.n)

fwrite(df.n, "/home/sergiy/Documents/Work/Nutricia/Data/N_MT_2015-2020M06.csv",
       row.names = FALSE)

### Step 4 Nielsen eCom

# Step 4.1. Read raw data
df = fread("/home/sergiy/Documents/Work/Nutricia/Data/eCommerce/N_EC_2020_M05.csv",
           check.names = TRUE)
df.sku.nielsen = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.nielsen.csv")
df.n.ec  = fread("/home/sergiy/Documents/Work/Nutricia/Data/N_MT_2015-2020M04.csv")

# Save a copy of existig descriptions
fwrite(df.sku.nielsen, 
       paste0("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.nielsen-", 
              Sys.Date(), ".csv"), 
       row.names = FALSE)

# Step 4.2. Define new descriptions
df.new = export.new.skus(df, df.sku.nielsen)

# Step 4.3. Append new descriptions to existing ones

# Just in case the data contain Size.pharma that is not used
# It is excluded by the following line
cols = names(df.new)[names(df.new) %in% names(df.sku.nielsen)]

df.sku.nielsen = rbindlist(list(df.sku.nielsen, df.new[, mget(cols)]), 
                           fill = TRUE)

# Step 4.4. Save updated file of descriptions
fwrite(df.sku.nielsen,
       "/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.nielsen.csv",
       row.names = FALSE)

# Step 4.5. Update sales file
df = raw.data.processing(df)
df.n = append.new.data(df, df.n, 0.20)
check.historical.data(df.n)

fwrite(df.n, "/home/sergiy/Documents/Work/Nutricia/Data/N_MT_2015-2020M05.csv",
       row.names = FALSE)

### Step 5. Check matrix of attributes
df.matrix = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.matrix.csv")

fwrite(df.matrix, 
       paste0("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.matrix ", 
              Sys.Date(), ".csv"), 
       row.names = FALSE)

check.sku.matrix(df.matrix)
check.attributes(df.matrix)

fwrite(df.matrix, "/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.matrix.csv",
       row.names = FALSE)

### Step 6. Generate pivot

