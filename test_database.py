import mysql.connector as sql
"""GO AND DO SOME CHANGES FOR REDUDANCY"""
mydb = sql.connect(									#connection to database
	host='localhost',
	user='root',
	passwd='password7',
	database='test'
	)
cur = mydb.cursor()									#cursor initialization

"""Creating department, admission,hostel and finalTable tables"""
def createTables():
	cur.execute("CREATE TABLE department(rno varchar(10) PRIMARY KEY,name varchar(20) NOT NULL,gender char NOT NULL,gpa integer(3),attandance integer(3))")
	cur.execute("CREATE TABLE admission(admsn_no varchar(10) PRIMARY KEY,name varchar(20) NOT NULL,gender varchar(6) NOT NULL,feedue integer(10))")
	cur.execute("CREATE TABLE hostel(allotment_no varchar(10) PRIMARY KEY,name varchar(20) NOT NULL,gender integer(1) NOT NULL,roomno integer(3) NOT NULL)")
	cur.execute("CREATE TABLE resultTable(rno varchar(10) PRIMARY KEY,name varchar(20),gender char)")

	print('-> Tables created Successfully')

"""Inserting data into the tables"""
def insertIntoTables():
	query1 = "INSERT INTO department(rno, name, gender) values(%s, %s, %s)"
	query2 = "INSERT INTO admission(admsn_no, name, gender) values(%s, %s, %s)"
	query3 = "INSERT INTO hostel(allotment_no, name, gender) values(%s, %s, %s)"
	"""Department Table"""
	students = []
	for _ in range(int(input('enter the number of records to insert into department table:\t'))):
		students.append((input('enter rno: '),input('enter name: '),input('enter gender(M:F): ')))
		print('-'*50)
	cur.executemany(query1,students)
	"""Admission Table"""
	students = []
	for _ in range(int(input('enter the number of records to insert into admission table:\t'))):
		students.append((input('enter admission no: '),input('enter name:'),input('enter gender(Male:Female):')))
		print('-'*50)
	cur.executemany(query2,students)
	"""Hostel Table"""
	students = []
	for _ in range(int(input('enter the number of records to insert into allotment table:\t'))):
		students.append((input('enter allotment_no:'),input('enter name:'),input('enter gender(0-Male:1-Female):')))
		print('-'*50)
	cur.executemany(query3,students)


	mydb.commit()
	print("-> Data inserted Successfully!")

"""Inserting all records from 3 Tables to a New Table """
def mergeTables():
	insertQuery = 'INSERT INTO resultTable(rno, name, gender) values (%s, %s, %s)'

	cur.execute("SELECT * FROM department")
	departmentTable = cur.fetchall()
	for record in departmentTable:
		cur.execute(insertQuery,(record[0],record[1],record[2]))

	cur.execute("SELECT * FROM admission")
	admissionTable = cur.fetchall()
	for record in admissionTable:
		if record[2].lower() == 'male':
			cur.execute(insertQuery,(record[0],record[1],'M'))
		else:
			cur.execute(insertQuery,(record[0],record[1],'F'))

	cur.execute("SELECT * FROM hostel")
	hostelTable = cur.fetchall()
	for record in hostelTable:
		if record[2] == '0':
			cur.execute(insertQuery,(record[0],record[1],'M'))
		else:
			cur.execute(insertQuery,(record[0],record[1],'F'))

	mydb.commit()
	print('-> Tables data inserted into resultTable Successfully!')
try:
	createTables()
	insertIntoTables()
	mergeTables()
except sql.errors.ProgrammingError as PE: #caught when we create a table with name that already exits in database
	print(PE)
except sql.errors.IntegrityError as IE:	#caught duplicate values while inserting into the table
	print (IE)
except sql.DatabaseError as DE:
	print(DE,'\n','Error:Value requried but found NULL value')
finally:
	cur.close()
	mydb.close() #closing the connection with database