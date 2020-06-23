import requests
from bs4 import BeautifulSoup
import json
import pymysql
import boto3
from PIL import Image
import time
import os
import copy

conn= pymysql.connect(host="yonsei-aurora-primary.cluster-cury5e9pcojj.ap-northeast-2.rds.amazonaws.com",
                             user="team_three",
                             password="berthurivts4mdv2ap",
                             port = 3306,
                             db='team_three',
                             cursorclass=pymysql.cursors.DictCursor)
cur= conn.cursor()
table_name='food_data_new'
key_id='AKIAXXPRRAWGIVPKHXSN'
secret_access_key='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'
bucket_name='team3-foodimage'
rating_bucket_name='team3-foodrating'

s3=boto3.client('s3',aws_access_key_id=key_id,aws_secret_access_key=secret_access_key,region_name='ap-northeast-2')


common_url='https://www.10000recipe.com'
list_num=0
len_results=True
total_results=[]
breaker=False

while len_results:
	list_num+=1
	try:
		recipe_list_url='/recipe/list.html?reco=date&page='
		recipe_list_url=common_url+recipe_list_url+str(list_num)
		while True:
				try:
					req=requests.get(recipe_list_url)
					break
				except ConnectionError:
					time.sleep(5)
					req=requests.get(recipe_list_url)
		html=req.text
		soup=BeautifulSoup(html,'html.parser')
		for result in soup.findAll('a', {'class':'thumbnail','href':True}):
			recipe_path=result['href']
			recipe_id=int(recipe_path.replace('/recipe/',''))
			cur.execute("select exists(select * from food_data_new where id=%i) as exist"%recipe_id)
			sql_result=cur.fetchone()
			if sql_result['exist']==1:
				breaker=True
				break
			total_results.append(recipe_path)
	except:
		print('recipe order %i was error!'%list_num)
	if breaker:
		break
	if list_num==3:
		break

print(len(total_results))




data_num=1
for result in total_results:
	if data_num==101:
		break
	try:
		recipe_url=result
		recipe_url=common_url+recipe_url
		req=requests.get(recipe_url)
		html=req.text
		soup=BeautifulSoup(html,'html.parser')
		food_dat_dict={}
		for name in soup.findAll('div',{'class':'view2_summary'}):
			name=name.text.strip()
			name=name.split('\n')[0]
			food_dat_dict['name']=name
		for ingre in soup.findAll('div',{'class':'cont_ingre'}):
			ingre1=ingre.text.strip()
			ingre1=ingre1.split('\n')
			ingre1=list(filter(lambda a: a!='',ingre1))
			ingre1=' '.join(ingre1)
			food_dat_dict['ingredient']=ingre1
		for ingre in soup.findAll('div',{'class':'cont_ingre2'}):
			ingre2=ingre.text
			ingre2=ingre2.replace('\n\n재료Ingredients\n\n계량법 안내\n\n\n\n\n','')
			ingre2=ingre2.split('\n\n')
			ingre2_new=[]
			for index in range(len(ingre2)):
				component=ingre2[index]
				if component==(''or'\n'):
					pass
				else:
					component=component.split()
					component=' '.join(component)
					ingre2_new.append(component)
			ingre2_new=list(filter(lambda a: a!='',ingre2_new))
			total_ingre={}
			category_num=0
			category_ingre=[]
			for ingre in ingre2_new:
				if '[' in ingre:
					category_num+=1
					category_ingre.append(ingre)
					total_ingre[category_ingre[category_num-1]]=[]
				else:
					total_ingre[category_ingre[category_num-1]].append(ingre)
			ingre2=''
			for ingre in total_ingre:
				ingre2=ingre2+ingre+' '+', '.join(total_ingre[ingre])+' '
			food_dat_dict['ingredient']=ingre2
		step_num=0
		tip_soup=soup.findAll('p',{'class':'step_add add_tip2'})
		while True:
			step_num+=1
			step_soup=soup.findAll('div',{'class':'view_step_cont media step%i'%step_num})
			len_soup=len(step_soup)
			if len_soup==0:
				break
			for step in step_soup:
				try:
					food_dat_dict['step_img_%i'%step_num]=step.img['src']
				except:
					food_dat_dict['step_img_%i'%step_num]='NULL'
				step=step.text
				step=step.replace('\n',' ')
				for tip in tip_soup:
					tip=tip.text
					if tip in step:
						step=step.replace(tip,'')
						step=step+'. tip) '+tip
			food_dat_dict['step_%i'%step_num]=step
		for main_pic in soup.findAll('div',{'class':'centeredcrop'}):
			food_dat_dict['main_img']=main_pic.img['src']
		id_=result
		id_=int(id_.replace('/recipe/',''))
		for tip in soup.findAll('dl',{'class':'view_step_tip'}):
			tip=tip.text
			tip=tip.strip()
			food_dat_dict['tip']=tip
			break
		try:
			food_dat_dict['tip']
		except KeyError:
			food_dat_dict['tip']='NULL'
		directory_name = "%s"%id_ #it's name of your folders
		column_insert_table_string='INSERT INTO %s(id,'%table_name
		value_insert_table_string='\nVALUES(%i,'%id_
		food_dat_dict_new=copy.deepcopy(food_dat_dict)
		for i in food_dat_dict_new:
			if 'img' in i:
				if food_dat_dict_new[i]=='NULL':
					pass
				else:
					byte_source=food_dat_dict_new[i]
					byte_source=byte_source.split('/')
					image_file_name=byte_source[-1]
					if '.jpg' in image_file_name:
						image_file_name=image_file_name.replace('.jpg','.png')
					elif '.gif' in image_file_name:
						image_file_name=image_file_name.replace('.gif','.png')
					elif '.JPG' in image_file_name:
						image_file_name=image_file_name.replace('.JPG','.png')
					food_dat_dict_new[i]='%s,%s/%s'%(bucket_name,id_,image_file_name)
		for i in food_dat_dict_new:
			if food_dat_dict_new[i]=='NULL':
				column_insert_table_string=column_insert_table_string+i+','
				value_insert_table_string=value_insert_table_string+"%s"%food_dat_dict_new[i]+','
			else:
				column_insert_table_string=column_insert_table_string+i+','
				value_insert_table_string=value_insert_table_string+'"%s"'%food_dat_dict_new[i]+','
		index=column_insert_table_string.rfind(',')
		column_insert_table_string=column_insert_table_string[:index]+')'
		index=value_insert_table_string.rfind(',')
		value_insert_table_string=value_insert_table_string[:index]+')'
		insert_string=column_insert_table_string+value_insert_table_string
		cur.execute(insert_string)
		conn.commit()
		s3.put_object(Bucket=bucket_name, Key=(directory_name+'/'))
		for i in food_dat_dict:
			if 'img' in i:
				byte_source=food_dat_dict[i]
				url=byte_source
				byte_source=byte_source.split('/')
				image_file_name=byte_source[-1]
				if food_dat_dict[i]=='NULL':
					pass
				else:
					while True:
						try:
							req=requests.get(url)
							time.sleep(5)
							break
						except:
							time.sleep(5)
							req=requests.get(url)
					food_image_content=req.content
					with open(image_file_name,'wb') as save_image:
						save_image.write(food_image_content)
					if '.jpg' in image_file_name:
						im=Image.open(image_file_name)
						image_file_name_temp=image_file_name
						image_file_name=image_file_name.replace('.jpg','.png')
						im.save(image_file_name)
						os.remove(image_file_name_temp)
					elif '.JPG' in image_file_name:
						im=Image.open(image_file_name)
						image_file_name_temp=image_file_name
						image_file_name=image_file_name.replace('.JPG','.png')
						im.save(image_file_name)
						os.remove(image_file_name_temp)
					elif '.gif' in image_file_name:
						im=Image.open(image_file_name).convert('RGB')
						image_file_name_temp=image_file_name
						image_file_name=image_file_name.replace('.gif','.png')
						im.save(image_file_name)
						os.remove(image_file_name_temp)
					s3.upload_file(image_file_name,bucket_name,'%s/%s'%(id_,image_file_name))
					os.remove(image_file_name)
		data_num+=1
	except:
		print('recipe path %s was error'%result)