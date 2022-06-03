import mysql.connector as connection

class SqlOperation:
    def __init__(self,host = None, user_name=None, password=None):
        try:
            if user_name is None or password is None or host is None:
                self.host = "localhost"
                self.user_name = "root"
                self.password = "Jaypr@1234"
                # print("password")
            else:
                self.host = host
                self.user_name = user_name
                self.password = password
        except Exception as e:
            print(str(e))

    def connectToMysql(self):
        try:
            mydb = connection.connect(host=self.host, user=self.user_name, passwd=self.password, use_pure=True)
            # check if the connection is established
            print(mydb.is_connected())
            mydb.close()
        except Exception as e:
            print(str(e))

    def checkListOfDatabases(self):
        try:
            mydb = connection.connect(host=self.host, user=self.user_name, passwd=self.password, use_pure=True)
            # check if the connection is established

            query = "SHOW DATABASES"

            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            print(cursor.fetchall())

        except Exception as e:
            mydb.close()
            print(str(e))

    def createDatabase(self, databasename):
        try:
            mydb = connection.connect(host=self.host, user=self.user_name, passwd=self.password,use_pure=True)
            # check if the connection is established
            print(mydb.is_connected())

            query = "Create database "+ databasename + ";"
            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            print("Database Created!!")
            mydb.close()
        except Exception as e:
            mydb.close()
            print(str(e))

    def selectFromDB(self, databasename):
        try:
            mydb = connection.connect(host=self.host, database = databasename,user=self.user_name, passwd=self.password,use_pure=True)
            #check if the connection is established
            print(mydb.is_connected())
            query = "Select * from " + databasename + ";"
            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            for result in cursor.fetchall():
                print(result)
            mydb.close() #close the connection


        except Exception as e:
            #mydb.close()
            print(str(e))

    def insertIntoUserTable(self, databaseName, user_id, uname, dob, email, created_date):
        try:
            mydb = connection.connect(host=self.host, database = databaseName,user=self.user_name, passwd=self.password,use_pure=True)
            # check if the connection is established
            print(mydb.is_connected())
            # query = "INSERT INTO "+ TableName + " VALUES ('1132','Sachin','Kumar','1997-11-11','Eleventh','A')"
            query = f"INSERT INTO user_table VALUES ({user_id}, '{uname}', '{dob}', '{email}', '{created_date}')"
            print(query)

            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            print("Values inserted into the table!!")
            mydb.commit()
            mydb.close()
        except Exception as e:
            mydb.close()
            print(str(e))


    def insertIntoBankAccountTable(self, databaseName, user_id, bank_id, bank_account_number, isactiave, amount):
        try:
            mydb = connection.connect(host=self.host, database = databaseName,user=self.user_name, passwd=self.password,use_pure=True)
            # check if the connection is established
            print(mydb.is_connected())
            query = f"INSERT INTO bank_account_table VALUES ({user_id}, {bank_id}, '{bank_account_number}', '{isactiave}', {amount})"
            print(query)

            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            print("Values inserted into the table!!")
            mydb.commit()
            mydb.close()
        except Exception as e:
            mydb.close()
            print(str(e))


    def insertIntoTransactionTable(self, databaseName, tranaction_date, user_id, bank_id, withdraw_amount):
        try:
            mydb = connection.connect(host=self.host, database = databaseName,user=self.user_name, passwd=self.password,use_pure=True)
            # check if the connection is established
            print(mydb.is_connected())
            query = f"INSERT INTO transaction_table VALUES ('{tranaction_date}', {user_id}, {bank_id},{withdraw_amount})"
            print(query)

            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            print("Values inserted into the table!!")
            mydb.commit()
            mydb.close()
        except Exception as e:
            mydb.close()
            print(str(e))

    def selectAllFromTable(self, databaseName, TableName):
        try:
            mydb = connection.connect(host=self.host, database = databaseName,user=self.user_name, passwd=self.password,use_pure=True)
            #check if the connection is established
            print(mydb.is_connected())
            query = "Select * from "+ str(TableName) +";"
            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            for result in cursor.fetchall():
                print(result)
            mydb.close() #close the connection


        except Exception as e:
            #mydb.close()
            print(str(e))

    def checkAccountBalance(self, databaseName, account):
        try:
            mydb = connection.connect(host=self.host, database = databaseName,user=self.user_name, passwd=self.password,use_pure=True)
            #check if the connection is established
            print(mydb.is_connected())
            query = f"Select * from bank_account_table where 'Acount number'= '{account}';"
            print(query)

            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            print("executed")
            print(cursor.fetchall())
            for result in cursor.fetchall():
                print(result)
            mydb.close() #close the connection


        except Exception as e:
            #mydb.close()
            print(str(e))


    def withdrawAmount(self, databaseName,account_number, withdrw_amount):
        try:

            mydb = connection.connect(host=self.host, database = databaseName,user=self.user_name, passwd=self.password,use_pure=True)
            # check if the connection is established
            print(mydb.is_connected())
            query = f"UPDATE bank_account_table SET amount = amount - {withdrw_amount} WHERE 'Account number' = '{account_number}' and amount > 5000"
            print(query)

            cursor = mydb.cursor()  # create a cursor to execute queries
            cursor.execute(query)
            print(cursor.fetchmany())
            mydb.commit()

            #let's check if the value is updated in the table.
            query = "Select * from bank_account_table where `Account number` = "+ str(account_number)
            cursor = mydb.cursor()  # create a cursor to execute queries
            cursor.execute(query)
            for result in cursor.fetchall():
                print(result)

            mydb.close()  # close the connection

        except Exception as e:
            #mydb.close()
            print(str(e))

    
    def checkTransaction(self, databaseName, startdate, enddate):
        try:
            mydb = connection.connect(host=self.host, database = databaseName,user=self.user_name, passwd=self.password,use_pure=True)
            #check if the connection is established
            print(mydb.is_connected())
            query = f"Select * from transaction_table where 'Transaction date' between '{startdate}' and '{enddate}'"
            print(query)

            cursor = mydb.cursor() #create a cursor to execute queries
            cursor.execute(query)
            for result in cursor.fetchall():
                print(result)
            mydb.close() #close the connection


        except Exception as e:
            #mydb.close()
            print(str(e))


        
