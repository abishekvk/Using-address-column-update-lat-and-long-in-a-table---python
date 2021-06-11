# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 12:25:29 2019

@author: avriyan
"""

from  geopy.geocoders import Nominatim
import pandas as pd
geolocator = Nominatim()
import cx_Oracle

class Geo_locator:
    def geo_locator(self,host,port,user,psw,service,tb_name,address_col_name,lat_col_name,long_col_name):
        #quoted = urllib.parse.quote_plus("DRIVER="+driver+";SERVER="+servr+";DATABASE="+db+";UID="+usrid+";PWD="+psw) 
	#engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
        CONN_INFO={'host': host,'port': port,'user': user,'psw': psw,'service': service}
        print(CONN_INFO)
        CONN_STR = '{user}/{psw}@{host}{port}/{service}'.format(**CONN_INFO)
        print(CONN_STR)
        #dsn=cx_Oracle.makedsn(host,port,service)
        dsn=cx_Oracle.makedsn(host, port, service_name=service)
        print(dsn)
        try:
            #conn = cx_Oracle.connect(CONN_STR)
            conn = cx_Oracle.connect(user=user, password=psw, dsn=dsn)
            conn.autocommit=True
            cur=conn.cursor()
            print("success")
        except Exception as e:
            print(str(e))
        sql_command="""SELECT * FROM """+tb_name+"WHERE ROWNUM<=10"
		#print(sql_command)
        print(sql_command)
        try:
            #df = pd.read_sql(sql_command, cur)
            df=pd.read_sql(sql_command, con=conn)
            print(df)
        except Exception as ex:
            print(str(ex))
            #res=cur.execute(sql_command)
        #df = DataFrame(resoverall.fetchall())		   
        #df.head()
        for index,row in  df.iterrows():
            address=row[address_col_name]
            print(address)
            try:
                location = geolocator.geocode('6 VERIZON LN, LANSING, NY 14882')
                print(location)
                statement = 'UPDATE '+tb_name+' set '+ lat_col_name + ' = ' + str(location.latitude)+ ' , ' +long_col_name + ' = ' +str(location.longitude)+' WHERE Address='+"'"+str(address)+"'"
                print(statement)
                #con.commit
				#print("Row updated")
                cur.execute(statement)
                conn.commit
                sql_command="""SELECT """+ lat_col_name + ',' + long_col_name +""" FROM """+tb_name+" WHERE address= "+address
                cur.execute(sql_command)
				#engine.execute('UPDATE '+tb_name+' set Lat='+str(location.latitude)+',Long='+str(location.longitude)+'WHERE Address='+"'"+str(address)+"'")
				#print(location.latitude,location.longitude)
				#print((row[Lat], row[Long]))
            except Exception as e:
                print(e)
                pass
        
        conn.close
            #str=row['Address']
            
	    #print('UPDATE Geo_data set Lan='+str(location.latitude))+',Long='+location.longitude+'WHERE Address='+address)
if __name__ == '__main__':
	host='abc.xyz.com'			 #server or host
	port='1521'			 #port number
	user='user1'			 #user name
	psw='Psw@123'			  #password
	service='SP1'		  #GIVE SERVICE NAME OR SID
	tb_name='Table_name'		  #TABLE NAME
	address_col_name='ADDRESS'  #column name which contains the address
	lat_col_name= 'C_LAT'		   #column name which contains latitude
	long_col_name= 'C_LONG'		 #column name which contains Longitude
	Geo_loc = Geo_locator()
	Geo_loc.geo_locator(host,port,user,psw,service,tb_name,address_col_name,lat_col_name,long_col_name)