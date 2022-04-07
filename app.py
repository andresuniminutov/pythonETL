# import mysql.connector
import pymysql
import datetime

def insertDataV2(line):
  try:
    conexion = pymysql.connect(host='localhost',
                              user='bigdata',
                              password='bigdata',
                              db='db_covid_big_data')
    try:
      with conexion.cursor() as cursor:
        consulta = "INSERT INTO tb_users (us_file_id, us_name,us_lastname,us_email,us_borndate,us_processdate,us_owner) VALUES (%s, %s,%s,%s,%s,%s,%s)"
        #Podemos llamar muchas veces a .execute con datos distintos
        val = (line[0], line[1],line[2],line[3],line[4],line[5],line[6])  
        #print(consulta)
        cursor.execute(consulta, val)
        
      conexion.commit()
    finally:
      conexion.close()
  except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
    print("Ocurrio un error al conectar: ", e)

f = open("dataset.csv", "r")
# print(f.read())

for x in f:
  line = x.split(";")
  id = line[0]  
  name = line[1]  
  lastname = line[2]  
  email = line[3]  
  bornDate = line[4]  
  line.append(datetime.datetime.now())
  line.append("me")
  insertDataV2(line) 




