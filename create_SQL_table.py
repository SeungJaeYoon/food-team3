import pymysql
connection = pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                             user="team_three",
                             password="berthurivts4mdv2ap",
                             port = 3306,
                             db='team_three',
                             cursorclass=pymysql.cursors.DictCursor)

cur= connection.cursor()


table_create_string='CREATE TABLE user_rating (\n'

cur.execute("select id from food_data_new")
result=cur.fetchall()
for i in result:
	table_create_string+=" `%s` int,"%i['id']
table_create_string+=' id int,'
table_create_string+=' PRIMARY KEY (id));'

cur.execute(table_create_string)
connection.commit()
