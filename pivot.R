library(data.table)
library(stringi)

df.n = fread("/home/sergiy/Documents/Work/Nutricia/Data/N_MT_2015-2020M03.csv")
df.p  = fread("/home/sergiy/Documents/Work/Nutricia/Data/BF_PH_2015-2020M03.csv")
df.amn = fread("/home/sergiy/Documents/Work/Nutricia/Data/AMN_PH_2015-2020M03.csv")
df.sku.proxima = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.proxima.csv")
df.sku.nielsen = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.nielsen.csv")
df.matrix = fread("/home/sergiy/Documents/Work/Nutricia/Data/Dictionaries/df.sku.matrix.csv")
dictRegions = fread("/home/sergiy/Documents/Work/Nutricia/Scripts/Pivot4/dictRegions.csv")
dictEC = fread("/home/sergiy/Documents/Work/Nutricia/Scripts/Pivot4/dictEC.csv")
dictAC = fread("/home/sergiy/Documents/Work/Nutricia/Scripts/Pivot4/dictAC.csv")
dict.price.segments = fread("/home/sergiy/Documents/Work/Nutricia/Scripts/Pivot4/PriceSegments.csv")
dictScent = fread("/home/sergiy/Documents/Work/Nutricia/Scripts/Pivot4/dictScents.csv")

# Matrix
df.matrix[, SKU2 := SKU]
# df.matrix[SKU2 == "OTHER ITEMS PRIVATE LABEL", SKU2 := paste(SKU2, DANONE.SUB.SEGMENT)]

# Nielsen
# df.n[SKU2 == "OTHER ITEMS PRIVATE LABEL", SKU2 := paste(SKU2, DANONE.SUB.SEGMENT)]
# df.n = df.n[, .(SKU2, Region, Ynb, Mnb, Volume, Value)]

# Convert regions

df.n[df.sku.nielsen, on = "SKU2", ID := i.ID]

df.n[is.na(ID) | ID == "", .N]
df.n = df.n[ID > 0]


df.n = df.n[df.matrix, on = "ID",
            `:=`(Brand = i.Brand,
                 SubBrand = i.SubBrand,
                 Organic = i.Organic,
                 CSS = i.CSS,
                 Items.in.pack = i.Items.in.pack,
                 Size = i.Size,
                 Age = i.Age,
                 Scent = i.Scent,
                 Protein = i.Protein,
                 Flavoured = i.Flavoured,
                 Company = i.Company,
                 PS0 = i.PS0,
                 PS2 = i.PS2,
                 PS3 = i.PS3,
                 PS = i.PS,
                 PSV = i.PSV,
                 Form = i.Form,
                 Package = i.Package,
                 Storage = i.Storage,
                 PromoPack  = i.PromoPack)]

df.n[, c("SKU2", "SKU") := NULL]
# names(df.n)[1] = "Region.MT"

# Pharma
df.amn = df.amn[Category.pharma != "ДП", 
                .(ID.morion, SKU2, Region.pharma, Ynb, Mnb, Items, Value)]
df.p = df.p[, .(ID.morion, SKU2, Region.pharma, Ynb, Mnb, Items, Value)]

all(names(df.amn) == names(df.p))

df.p = rbindlist(list(df.p, df.amn), use.names = TRUE)

df.p[df.sku.proxima, on = c("ID.morion", "SKU2"), ID := i.ID]
df.p[is.na(ID) | ID == "", .N]
# df.p[ID == -1, .N]
# df.p[ID == -1, unique(SKU2)]

df.p = df.p[ID > 0]

df.p = df.p[df.matrix, on = "ID",
            `:=`(Brand = i.Brand,
                 SubBrand = i.SubBrand,
                 Organic = i.Organic,
                 CSS = i.CSS,
                 Items.in.pack = i.Items.in.pack,
                 Size = i.Size,
                 Age = i.Age,
                 Scent = i.Scent,
                 Protein = i.Protein,
                 Flavoured = i.Flavoured,
                 Company = i.Company,
                 PS0 = i.PS0,
                 PS2 = i.PS2,
                 PS3 = i.PS3,
                 PS = i.PS,
                 PSV = i.PSV,
                 Form = i.Form,
                 Package = i.Package,
                 Storage = i.Storage,
                 PromoPack  = i.PromoPack)]

df.p[, c("SKU2", "ID.morion") := NULL]

# df.p = df.p[!is.na(Brand)]
# df.n = df.n[!is.na(Brand)]

calculate.volume(df.p)

df.p[, Items := NULL]

# Convert regions

df.n[dictRegions[Channel == "MT"], 
     on = c(Region.nielsen = "Region.channel"),
     Region := i.Region]

df.n = df.n[!is.na(Region)]


df.p[dictRegions[Channel == "PHARMA"], 
     on = c(Region.pharma = "Region.channel"),
     Region := i.Region]

df.p[is.na(Region), .N]

# Join tables
df.n[, "Region.nielsen" := NULL]
df.p[, "Region.pharma" := NULL]

df.n[, Channel := "MT"]
df.p[, Channel := "PHARMA"]

# df.n[, c("BRAND",
#          "BRAND.OWNER",
#          "DANONE.SEGMENT",
#          "DANONE.SUB.SEGMENT",
#          "PRODUCT.FORM",
#          "TYPE...BABY.PRODUCT",
#          "PRODUCT.BASE",
#          "Period",
#          "Scent2",
#          "ScentType") := NULL]

all(names(df.n) %in% names(df.p))

df = rbindlist(list(df.p, df.n), use.names = TRUE)

# Add SKUs

df[Items.in.pack == 1,
   SKU := paste(Brand, SubBrand, Size, Age, Scent, PS, Form, Package, PromoPack)]
df[Items.in.pack != 1,
   SKU := paste0(Brand, " ", SubBrand, " ", Items.in.pack, "*", Size, 
                 " ", Age, " ", Scent, " ", PS, " ", Form, " ", Package, 
                 " ", PromoPack)]

df[Brand == "Private Label", SKU := "Private Label SKUs"]

# Add Scent2 & ScentType

df.matrix[, 
          Scent2 := ifelse(PS3 == "Instant Cereals", 
                           mapply(unlist(define.scent.type), Scent)[1],
                           ""), by = 1:nrow(df.matrix)]

df.matrix[, 
          ScentType := ifelse(PS3 == "Instant Cereals", 
                              mapply(unlist(define.scent.type), Scent)[2],
                              ""), by = 1:nrow(df.matrix)]

df[df.matrix, on = "ID", `:=`(Scent2 = i.Scent2,
                              ScentType = i.ScentType)]


df = df[, .(Volume = sum(Volume), 
            Value = sum(Value)), 
        by = .(SKU, Brand, SubBrand, Organic, CSS, Items.in.pack, Size,
            Age, Scent, Scent2, ScentType, Protein, Flavoured, Company,
            PS0, PS2, PS3, PS, PSV,
            Form, Package, Storage, PromoPack, 
            Region, Channel, Ynb, Mnb)]

extrapolate(df)

add.price.segments(df)

add.acidified(df)

df = df[, .(SKU, Brand, SubBrand, Organic, CSS, Items.in.pack, Size,
            Age, Scent, Scent2, ScentType, Acidified, Protein, Flavoured, Company,
            PS0, PS2, PS3, PS, PSV,
            Form, Package, Storage, PromoPack, PriceSegment, GlobalPriceSegment,
            Region, Channel, Ynb, Mnb, 
            Volume, Value, EC, AC, VolumeC, ValueC)]

fwrite(df,
       "/home/sergiy/Documents/Work/Nutricia/Data/202003/df.csv",
       row.names = FALSE)

fwrite(df[, .(SKU, Brand, SubBrand, Size,
              Age, Scent, Scent2, ScentType, Acidified, Company,
              PS0, PS2, PS3, PS, 
              Form, Package, PromoPack, PriceSegment,
              Region, Channel, Ynb, Mnb, 
              Volume, Value, EC, AC, VolumeC, ValueC)],
       "/home/sergiy/Documents/Work/Nutricia/Data/202003/df.pivot.csv",
       row.names = FALSE)


fwrite(df[Ynb >= 2018], 
       "/home/sergiy/Documents/Work/Nutricia/Data/202003/df.short.csv", 
       row.names = FALSE)

# df = df[, .(SKU, Brand, SubBrand, Organic, CSS, Items.in.pack, Size,
#             Age, Scent, Protein, Flavoured, Company,
#             PS0, PS2, PS3, PS, PSV,
#             Form, Package, Storage, PromoPack, 
#             Region, Ynb, Mnb, Volume, Value)]


# df.matrix[, Scent := sapply(stri_split_fixed(Scent, "-"), function(x)
#    paste(sort(x), collapse = "-"))]
