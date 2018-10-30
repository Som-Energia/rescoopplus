year=2018
for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
  wget http://datosclima.es/capturadatos/Aemet${year}-${month}.rar
done
for datafile in *.rar; do unrar x $datafile; done

