# Rutas relativas
rutaBase = "C:/Users/eso/Documents/UFV/TFC/datasetGDELT"
# Funcion para cargar los datos desde una ruta indicada
CargarDatos <- function(ruta,f){
  ruta=paste(rutaBase,"files",sep="/")
  ruta=paste(ruta,"Data",f,sep="/")
  ruta=paste(ruta,"csv",sep=".")
    dataset1 = read.csv(file=ruta,header = TRUE,stringsAsFactors = FALSE, sep=',')
    return(dataset1)
} 
## Leemos los datos 
ds1 <- CargarDatos(rutaBase,"gdeltv2_events_esp_0")
## Preparamos los datos de ambos 
# Filtramos los datos  innecesarios--selecciono columnas de cod  actores
ds2 <- subset (ds1, select = c (Actor1Code, Actor2Code))
#Codificacion del Dataset #limpieza dataset quitando NA y NULOS
  
#| es un or-operador y ! invierte. Sin embargo, el commando muestra todas las filas, que no son  b) NA o b) igual a ""
ds2<- ds2[!(is.na(ds2$Actor1Code) | ds2$Actor1Code==""), ]
ds2<- ds2[!(is.na(ds2$Actor2Code) | ds2$Actor2Code==""), ]
ds.act.values<-ds2
#seleccion 1 columna ds
ds1_c1<- subset(ds2,select = c(Actor1Code))
nuevo_orden=sort.list(ds1_c1$Actor1Code)
#ordeno alfabeticamente valores ds                 
sort1.hsb2 <- ds1_c1[ , order(c(names(ds1_c1)))]
#Partimos de un vector de string y deseamos dividir ese string en palabras y posteriormente crear un data frame de una sola columna con tantos elementos como palabras tenga nuestro vector de cadenas de texto
#Este es nuestro elemento inicial. Vamos a transformar un texto en una tabla de palabras. Tenemos que generar un data frame con con las palabras que componen el vector.
#función strsplit para crear una lista de palabras con los elementos del vector inicial.
#Es importante el uso de unlist para realizar el proceso correctamente.
Actor1Code = strsplit(sort1.hsb2, split=" ")
texto_columnas = data.frame(unlist(Actor1Code))
#quito valores duplicados
tst= texto_columnas[!duplicated(texto_columnas), ]
tst_dp=data.frame(unlist(tst))
#renombro columna
colnames(tst_dp) <- "Ac1CodSinDuplicados"
#añado una columna con valores enteros
tst_dp$valor  <- 1:nrow(tst_dp)
##almaceno tabla con valores de los Actores GDELT
write.csv(tst_dp,'ActCode.csv',row.names = FALSE)
#macheo valores numericos
ds1_c1$Actor1Code <- tst_dp$valor[match(ds1_c1$Actor1Code, tst_dp$Ac1CodSinDuplicados)]
#cogo la segunda columna Actor2Code
ds1_c2<- subset(ds2,select = c(Actor2Code))
ds1_c2$Actor2Code <- tst_dp$valor[match(ds1_c2$Actor2Code, tst_dp$Ac1CodSinDuplicados)]
#union los 2 ds, con los numeros asociados
ds_num<-cbind(ds1_c1,ds1_c2)
#quito valores nulos
ds_num<- ds_num[!(is.na(ds_num$Actor1Code) | ds_num$Actor1Code==""), ]
ds_num<- ds_num[!(is.na(ds_num$Actor2Code) | ds_num$Actor2Code==""), ]
#Escribo a csv el dataset resultante write.csv(ds2,'ds2.csv')
write.csv(ds_num,'dsnum.csv',row.names = FALSE)
