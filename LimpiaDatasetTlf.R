# Rutas relativas
rutaBase = "C:/Users/eso/Documents/UFV/TFC/datasetTlf"
# Funcion para cargar los datos desde una ruta indicada
CargarDatos <- function(ruta,f){
  ruta=paste(rutaBase,"files",sep="/")
  ruta=paste(ruta,"Data",f,sep="/")
  ruta=paste(ruta,"csv",sep=".")
  dataset1 = read.csv(file=ruta,header = TRUE,stringsAsFactors = FALSE, sep=',')
  return(dataset1)
} 
## Leemos los datos 
ds1 <- CargarDatos(rutaBase,"Listadotlf")
## Preparamos los datos de ambos 
# Filtramos los datos  innecesarios—selección columnas actores
ds2 <- subset(ds1,select = c(TEL.1,TEL.2))
#Codificacion del Dataset
#limpieza dataset quitando NA y NULOS
#| es un or-operador y ! invierte. Sin embargo, el commando muestra todas las filas, que no son  b) NA o b) igual a ""
ds2<- ds2[!(is.na(ds2$TEL.1) | ds2$TEL.1==""), ]
ds2<- ds2[!(is.na(ds2$TEL.2) | ds2$TEL.2==""), ]
ds.tlf.values<-ds2
#Escribo a csv el dataset resultante 
write.csv(ds2,'ds2.csv')
write.csv(ds.tlf.values,'dstlf.csv',row.names = FALSE)
