#!/usr/bin/python
from pymongo import *
from smtplib import *
import pymongo
import sys
import datetime
import shutil
import time
import os
import commands
import getpass
import urllib
import json
import logging
import smtplib
from deploycron import deploycron
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import argparse
from distutils import spawn
import re
#HOST_INFORMATION


def check_ip(ip):
  return [0<=int(x)<256 for x in re.split('\.',re.match(r'^\d+\.\d+\.\d+\.\d+$',ip).group(0))].count(True)==4


mongo_check = spawn.find_executable('mongodump')
if mongo_check is None:
  print('mongodump not installed\n')
  sys.exit()

parser = argparse.ArgumentParser('MongoDB Backup')
parser.add_argument('-f', '--file',help='put the json config file')
parser.add_argument('-r', '--reset',help="-r ALL 'delete all backups and logs from default'")

args = parser.parse_args()

if args.reset:
  args.reset = args.reset.lower()
  if args.reset == 'all':
    os.system('rm -r mongodb_backup')
    os.system('rm .delete')
    os.system('rm .dont_delete')
    os.system('crontab -r')
    print('\n all backups and logs deleted from default\n')
    sys.exit()
  else:
    print('\nchoose correct optioin for -r or --reset (all,ALL)\n')
    sys.exit()

if not args.file:
  print("\ngetting config file from current path")
  jc_file = os.path.join(os.getcwd(),'info.json')
else:
  jc_file = args.file

if not os.path.isfile(jc_file):
  print('\nno config file in this path\n')
  sys.exit()


if os.path.isfile(jc_file):
  try:
    data = json.load(open(jc_file))
  except:
    print('Json file (%s) is not valid \n'%(jc_file))
    logging.error('Json file (%s) is not valid \n'%(jc_file))
    sys.exit()
  if data.has_key('logpath'):
    logfile = data['logpath']
  else:
    logfile = 'mongo_backup.log'
 
  Format = '%(asctime)-15s %(message)s'
  logging.basicConfig(filename=logfile, format=Format, level=logging.DEBUG)

  if data.has_key('host'):
      host = data['host']
  else:
    host = raw_input('host : ')

  if check_ip(host) == True:
    pass
  else:
    print('Host ip is not valid : %s'%(host))
    logging.error('Host ip is not valid : %s\n'%(host))
    sys.exit() 

  if data.has_key('port'):
    port = data['port']
  else:
    port = raw_input('port : ')

  if data.has_key('db'):
    db = data['db']
    if db == 'all':
      collection = None
    elif data.has_key('collection'):
      collection = data['collection']
    else:
      collection = None
  else:
    db = None
  if data.has_key('retention'):
    retention = data['retention']
  else:
    retention = None
  if data.has_key('compression'):
    compression = data['compression']
  else:
    compression = None
  if data.has_key('backup_path'):
    backup_path = data['backup_path']
  else:
    backup_path = None
  if data.has_key('copy_into_remote_server'):
    rscp = data['copy_into_remote_server']
  else:
    rscp = None
  if data.has_key('backup_type'):
    backup_type = data['backup_type']
    backup_type = backup_type.lower()
  else:
    backup_type = None
  if data.has_key('feildnames'):
    feildnames = data['feildnames']
  else:
    feildnames = None
  if data.has_key('client'):
    client_name = data['client']
  else:
    client_name = 'Not Mentioned'
  if data.has_key('remote_host'):
    if check_ip(data['remote_host']) == True:
      pass
    else:
      print('remote_host is not valid : %s'%(data['remote_host']))
      logging.error('remote_host is not valid : %s\n'%(data['remote_host']))
      sys.exit()
  if data.has_key('remote_copy_method'):
    r_method = data['remote_copy_method'].lower()
    if r_method == 'rsync' or r_method == 'scp':
      pass
    else:
      print('remote_copy_method is not valid %s choose(rsync/scp)'%(data['remote_copy_method']))
      logging.error('remote_copy_method is not valid %s'%(data['remote_copy_method']))
      sys.exit()
else:
  host = raw_input('host : ')
  port = raw_input('port : ')

logging.info('\n\nset host: {} port : {}'.format(host,port))

#MAKE_CONNECTION
def make_conn(host,port,username=None,password=None):
  if username != None and password != None:
    uri = "mongodb://%s:%s@%s:%s"%(username,password,host,port)
    client = MongoClient(uri)
  else:
    client = MongoClient("host:port".format(host=host,port=port))
  return client


#MAKE_DIR_AND_FILE
def mk_dir(backup_path=None):
  if backup_path == None or backup_path == '':
    path = raw_input("Enter path : ")
  else:
    path = backup_path
  today = datetime.datetime.today()
  f_time = today.strftime('%Y_%m_%d_%H_%M')
  f_time = "mongodb_backup/mongo_backup_" + f_time
  if path == 'default' or path == '':
    f_path = str(os.path.join(os.getcwd(),f_time))
    if not os.path.exists(f_path):
      os.makedirs(f_path)
    else:
      print('wait for one minut ')
      with open('.dont_delete' 'r') as fin:
        data = fin.read().splitlines(True)
      dl = 0
      for i in data:
        dl = dl + 1
      with open('.dont_delete', 'w') as fout:
        fout.writelines(data[:dl])
  else:
    f_path = os.path.join(path,f_time)
    os.makedirs(f_path)
    if not os.path.exists(path):
      print("Incorrect Path")
      mk_dir()
  logging.info("directory created : {}".format(f_path))
  return f_path 

#SEND_EMAIL
def send_mail(message):
  try:
    msg = MIMEMultipart()
    msg['From'] = 'Mydbops'
    msg['Subject'] = 'MongoDB Backup Notification'
    msg.attach(MIMEText(message, 'html'))
    email_host = 'smtp.gmail.com'
    email_port = 587
    email_usr = 'rkm.hacker@gmail.com'
    email_pwd = 'rkm75656'
    to_list = ['rkm.hacker@gmail.com','ratneshm@mydbops.com']
    email_conn = smtplib.SMTP(email_host,email_port)
    email_conn.ehlo()
    email_conn.starttls()
    email_conn.login(email_usr,email_pwd)
    email_conn.sendmail('Mydbops',to_list,msg.as_string())
    email_conn.quit()
    print("Email sent ! ")
  except smtplib.SMTPAuthenticationError, ex:
    print 'Authenticion error!', ex
    print("Email sending failed ... ")
    email_conn.quit()
  except:
    print("Email sending failed ... ")
    email_conn.quit()

#HUMEN READABLE CONVERTER
def hum_con(value):
  kb = 1024.0
  mb = 1024.0**2
  gb = 1024.0**3
  if value < kb:
    value_f = '%.3fB'%(value)
  elif value >= kb and value < mb:
    value = value/kb
    value_f = '%.3fK'%(value)
  elif value >= mb and value < gb:
    value = value/mb
    value_f = '%.3fM'%(value)
  else:
    value = value/gb
    value_f = '%.3fG'%(value)
  return value_f

#DATABSE_ALL_INFO_PRINT
def db_info(client,db):
  dbs_size,tk_space,a_s = 0,13,13*' '
  k = 1024.0
  m = 1024.0**2
  g = 1024.0**3
  db_all = client.database_names()
  db_len = len(db_all)
  if db == None:
    print("            DATABASES         DB_SIZE(inByte)")
    print('   -------------------------------------------------')
  for i in db_all:
    take_space_ = tk_space - len(i)
    take_space = take_space_ * ' '
    db_size = client[i].command('dbstats')['dataSize']
    if db_size < k:
      db_size = '%.1fB'%(db_size)
    elif db_size >= k and db_size < m:
      db_size = db_size/k
      db_size = '%.1fK'%(db_size)
    elif db_size >= m and db_size < g:
      db_size = db_size/m
      db_size = '%.1fM'%(db_size)
    else:
      db_size = db_size/g
      db_size = '%.1fG'%(db_size)
    if db == None:
      print(a_s+'{}'.format(i)+take_space+'|\t{a}'.format(a=db_size))
    dbs_size = dbs_size + client[i].command('dbstats')['dataSize']
  dbs_size = hum_con(dbs_size)
  if db == None:
    print('   -------------------------------------------------')
    print('   total DB found : {}'.format(db_len) + ' | total_db_size : %s'%(dbs_size))
    print('   -------------------------------------------------')
  #SYSTEM_MEMORY_STATISTICS
    total_mem = commands.getstatusoutput("df -h . | awk '{print $2}' | head -n 2 | tail -1")[1]
    used_mem = commands.getstatusoutput("df -h . | awk '{print $3}' | head -n 2 | tail -1")[1]
    avial_mem = commands.getstatusoutput("df -h . | awk '{print $4}' | head -n 2 | tail -1")[1]
    a_m_m = commands.getstatusoutput("df -m . | awk '{print $4}' | head -n 2 | tail -1")[1]
    print("   Total_disk : {} | Used_disk : {} | Free_disk : {} ".format(total_mem,used_mem,avial_mem))
    print('   -------------------------------------------------\n')
  return dbs_size


#AUTHENTICATION_INFORMATION

try:
  get_conf = commands.getstatusoutput("ps -ef | grep -i mongod | grep -v color | awk '{print $NF}' | head -n 1")
  get_conf = get_conf[1]
  get_auth_info = commands.getstatusoutput("grep -i auth {a}".format(a=get_conf))
  get_auth_info = get_auth_info[1]
  get_auth_info = get_auth_info .split(':')
  get_auth_info = get_auth_info[1]
  get_auth_info = get_auth_info.replace(' ','')
  if get_auth_info == 'enabled':
    #print("Athentication enabled")
    if os.path.isfile(jc_file):
      logging.info("fatching loggin information from logininfo file ")
      data = json.load(open(jc_file))
      if data.has_key('usr'):
        username = data['usr']
      else:
        username = raw_input('username : ')
      if data.has_key('pwd'):
        password_ = data['pwd']
        password = urllib.quote_plus(password_)
      else:
        password_ = raw_input('Password : ')
        password = urllib.quote_plus(password_)
    else:
      username = raw_input('username : ')
      password_ = raw_input('Password : ')
      password = urllib.quote_plus(password_)
  #  username = raw_input('Username : ')
  #  username = urllib.quote_plus(username)
  #  password_ = getpass.getpass("Password : ")
  #  password = urllib.quote_plus(password_)
    client = make_conn(host,port,username,password)
  else:
    client = make_conn(host,port)
except pymongo.errors.PyMongoError,e:
  err_ =  "Error : %s" %e
  print(err_)
  logging.error("service failed : %s"%e)
  logging.critical("backup failed : %s"%e)
  send_mail(err_)
  sys.exit()
except:
  print("Service errror ")
  logging.error("service failed ")
  logging.critical("backup failed ")
  send_mail("Service errror \n Backup failed ")
  sys.exit()



try:
  dbs_size = db_info(client,db)
except pymongo.errors.PyMongoError,e:
  err_ =  "Error : %s" %e
  print(err_)
  logging.error("service failed : %s"%e)
  logging.critical("backup failed : %s"%e)
  send_mail(err_)
  sys.exit()

#print(username,"\n",password)
total_mem = commands.getstatusoutput("df -h . | awk '{print $2}' | head -n 2 | tail -1")[1]
used_mem = commands.getstatusoutput("df -h . | awk '{print $3}' | head -n 2 | tail -1")[1]
avial_mem = commands.getstatusoutput("df -h . | awk '{print $4}' | head -n 2 | tail -1")[1]
a_m_m = commands.getstatusoutput("df -m . | awk '{print $4}' | head -n 2 | tail -1")[1]
logging.info("total_disk : {}, used_disk: {}, avial_disk : {}".format(total_mem,used_mem,avial_mem))

#DB_COLL_SIZE_INFO
def db_coll_size(client,db_name, coll_name=None,collection=None):
  x,y,tc_size = 17,0,0
  k = 1024.0
  m = 1024.0**2
  g = 1024.0**3
  db = client[db_name]
  if coll_name != None:
    coll_size = db.command('collstats',coll_name)['size']
  else:
    collections = db.collection_names()
    if collection == None:
      print("\nSelected DB : {}\n".format(db_name))
      print('\t\t\tCOLLECTIONS\t\tCOLL_SIZE(inByte)')
      print('\t\t   -------------------------------------------------')
    for i in collections:
      space_ = x - len(i)
      y = y + 1
      space = space_ * ' '
      coll_size = db.command('collstats',i)['size']
      tc_size = tc_size + coll_size
      if coll_size < k:
        coll_size = '%.1fB'%(coll_size)
      elif coll_size >= k and coll_size < m:
        coll_size = coll_size/k
        coll_size = '%.1fK'%(coll_size)
      elif coll_size >= m and coll_size < g:
        coll_size = coll_size/m
        coll_size = '%.1fM'%(coll_size)
      else:
        coll_size = coll_size/g
        coll_size = '%.2fG'%(coll_size)
      if collection == None:
        print("\t\t\t {}".format(i)+space+"|       {}".format(coll_size))
    tc_size = hum_con(tc_size)
    if collection == None:
      print('\t\t   -------------------------------------------------')
      print('\t\t\tTotal Collections found       : {} '.format(y))
      print('\t\t\tTotal size of collections : %s'%(tc_size))
      print('\t\t   -------------------------------------------------')
      print('\t\t   Total_disk : {} | Used_disk : {}| Free_disk : {}'.format(total_mem,used_mem,avial_mem))
      print('\t\t   -------------------------------------------------\n')
  return tc_size


#BD COPY INTO SERVER
def cp_backup_into_server(s_path,jc_file):
  try:
    if os.path.isfile(jc_file):
      logging.info("fatching server information from logininfo file ")
      s_info = json.load(open(jc_file))
      if s_info.has_key('remote_user'):
        s_user = s_info['remote_user']
      else:
        s_user = raw_input('Server username : ')
      if s_info.has_key("remote_host"):
        s_host = s_info["remote_host"]
      else:
        s_host = raw_input('server Hostname : ')
      if s_info.has_key('port'):
        s_port = s_info['port']
      if s_info.has_key('remote_path'):
        d_path = s_info['remote_path']
      else:
        d_path = raw_input("Server path : ")
      if s_info.has_key('remote_copy_method'):
        rscp_method = s_info['remote_copy_method']
      else:
        rscp_method = raw_input("remote copy method ('rsync/') : ")
    else:
      s_user = raw_input('Server username : ')
      s_host = raw_input('server Hostname : ')
      s_port = s_info['port']
    print("\n===========================================================\n")
    print("Backuped Data copying into the server ...")
    #s_user = "{}".format(s_user)
    #s_host = "{}".format(s_host)
    #d_path = "{}".format(d_path)
    #print(s_path,s_user,s_host,d_path)
    print(commands.getoutput("{rscp_method} -r {s_path} {user}@{host}:{d_path}".format(rscp_method=rscp_method, s_path=s_path,user=s_user,host=s_host,d_path=d_path)))
    #print("Backuped Data copying into the server ...")
    time.sleep(0.8)
    print("Copying data completed .")
    print("\n===========================================================\n")
  except:
    print("copying data failed !")
    print("\n===========================================================\n")
  return s_path,s_user,s_host,d_path


#COMPARE COLL SIZE
def comp_coll_size(client,db_name, coll_name=None):
  x,y,tc_size = 17,0,0
  db = client[db_name]
  if coll_name != None:
    tc_size = db.command('collstats',coll_name)['size']
    tc_size = tc_size/1024.0
  else:
    collections = db.collection_names()
    for i in collections:
      y = y + 1
      coll_size = db.command('collstats',i)['size']
      tc_size = tc_size + coll_size
      tc_size = tc_size/1024.0 
  return tc_size



def taking_backup(host,port,uname,pwd,path,db_name=None,coll_name=None,comprs=None,backup_type=None,feildnames=None,logfile=None):
  if backup_type == 'json':
    try:
      if db_name != None and coll_name != None:
        os.system('mongoexport --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -d {db} -c {coll} -o {path}/{file}.json |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path,db=db_name,coll=coll_name,file=coll_name))
        process_ = os.system('grep Failed temp')
        os.system('cat temp | cat >> %s'%(logfile))
      else:
        print("must specify the 'db' and 'collection'")
    except:
      error_ = commands.getoutput('grep Failed temp')
      print("ERROR : %s"%(error_))
      logging.error("ERROR : %s"%(error_))
      send_mail(error_)
      sys.exit()
  elif backup_type == 'csv':
    try:
      if db_name != None and coll_name != None:
        os.system('mongoexport --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -d {db} -c {coll} -f {feildnames} --type csv -o {path}/{file}.json |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path,db=db_name,coll=coll_name,file=coll_name,feildnames=feildnames))
        process_ = os.system('grep Failed temp')
        os.system('cat temp | cat >> %s'%(logfile))
      else:
        print("must specify the 'db' and 'collection'")
    except:
      error_ = commands.getoutput('grep Failed temp')
      print("ERROR : %s"%(error_))
      logging.error("ERROR : %s"%(error_))
      send_mail(error_)
      sys.exit()
  elif backup_type == 'bson' or backup_type == '' or backup_type == None:
    if comprs == None or comprs == 'n' or comprs == 'N' or comprs == '':
      try:
        if db_name == None and coll_name == None:
          os.system('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -o {path} |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path))      
          #print('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -o {path} | mongo.log'.format(host=host,port=port,uname=uname,pwd=pwd,path=path))
          process_ = os.system('grep Failed temp')
          os.system('cat temp | cat >> %s'%(logfile))
        elif db_name != None and coll_name == None:
          process_ = os.system('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -d {db} -o {path} |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path,db=db_name))
          process_ = os.system('grep Failed temp')
          os.system('cat temp | cat >> %s'%(logfile))
        else:
          process_ = os.system('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -d {db} -c {coll} -o {path} |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path,db=db_name,coll=coll_name))
          process_ = os.system('grep Failed temp')
          os.system('cat temp | cat >> %s'%(logfile))
      except:
        error_ = commands.getoutput('grep Failed temp')
        print("ERROR : %s"%(error_))
        logging.error("ERROR : %s"%(error_))
        send_mail(error_)
        sys.exit()
    else:
      try:
        if db_name == None and coll_name == None:
          os.system('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin --gzip --archive={path} |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path))      
          #print('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -o {path} | mongo.log'.format(host=host,port=port,uname=uname,pwd=pwd,path=path))
          process_ = os.system('grep Failed temp')
          os.system('cat temp | cat >> %s'%(logfile))
        elif db_name != None and coll_name == None:
          process_ = os.system('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -d {db} --gzip --archive={path} |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path,db=db_name))
          process_ = os.system('grep Failed temp')
          os.system('cat temp | cat >> %s'%(logfile))
        else:
          process_ = os.system('mongodump --host {host} --port {port} -u {uname} -p {pwd} --authenticationDatabase admin -d {db} -c {coll} --gzip --archive={path} |& tee temp'.format(host=host,port=port,uname=uname,pwd=pwd,path=path,db=db_name,coll=coll_name))
          process_ = os.system('grep Failed temp')
          os.system('cat temp | cat >> %s'%(logfile))
      except:
        error_ = commands.getoutput('grep Failed temp')
        print("ERROR : %s"%(error_))
        logging.error("ERROR : %s"%(error_))
        send_mail(error_)
        sys.exit()
  else:
    print("Input the correct backup type (bson,json,csv): {}".format(backup_type))
  return process_   

def current_time():
  today = datetime.datetime.today()
  f_time = today.strftime('%Y-%m-%d %H:%M:')
  return f_time

def print_time():
  print_time=time.strftime('%Y-%m-%d %H:%M:%S %Z')
  return print_time

def take_backup(db,collection,compression,backup_path,retention=None,backup_type=None,feildnames=None,client_name=None,logfile=None):
#if a_m_m >= dbs_size :
#DATABASE_SELECT_AND_TAKE BACKUP
  css = """<style>
table {
    width:50%;
}
table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
}
th, td {
    padding: 3px;
    text-align: center;
}
table#t01 tr:nth-child(even) {
    background-color: #eee;
}
table#t01 tr:nth-child(odd) {
   background-color:#fff;
}
table#t01 th {
    background-color:#81c784;
    color: black;
}
</style>"""
  html_message = """<html><head>{css}</head>
Hi Team,<br><br><body>
Mydbops backup status for {client} - <font color="green"><b> success </b></font><br><br>
Backup directory   : {directory}<br><br>
Backup retention days : {retention}<br><br>
<center><h3>Available backups</h3>
<table id="t01">
<tr>
  <th>BACKUP PATH</th><th>SIZE</th><th>START TIME</th><th>END TIME</th><th>STATUS</th><th>BACKUP TYPE</th>
</tr>
{mail_column}
  </table></center><br><br>Regards,<br>Mydbops Team
  </html>"""
  c_time = current_time()
  #db_small,coll_small = '', ''
#  dbs_size = db_info(client)
  if db == None:
    db_name = raw_input("Type DB name (ALL/db_name) : ")
  else:
    db_name = db
  db_small = db_name.lower()
  #print(db_small)
  #print(collection)
  if db_small == 'exit':
    sys.exit()
  else:
    if db_small == 'all':
      #print(a_m_m,dbs_size)
      if a_m_m >= dbs_size:
        path = mk_dir(backup_path)
        b_strt=print_time()
        logging.info("BACKUP STARTED : %s"%(b_strt))
        start_backup ='\nBackup started : %s\n' %(b_strt)
        if compression == None:
          comprs = raw_input("Do you want to compressed backup(Y/N) : ")
        else:
          comprs = compression
        if comprs == 'n' or comprs == 'N':
          print(start_backup)
          process_status = taking_backup(host, port, username, password_ , path,backup_type=backup_type,feildnames=feildnames,logfile=logfile)
        else:
          print(str(start_backup) + "with compression")
          process_status = taking_backup(host, port, username, password_ , path,comprs='yes',backup_type=backup_type,feildnames=feildnames,logfile=logfile)
        b_end=print_time()
        #print process_
        if process_status != 0:
          b_comp1='\nBackup Succesfully completed : %s\n' %(b_end)
          print(b_comp1)
          bd_size = commands.getoutput('du -sh {}'.format(path)).split('\t')[0]
          del_path = path
          delete_path = ''
          #print(del_path,"------------")
          del_path = del_path.split('/')
          #print(del_path,"------------")
          del_path.pop(0)
          blast_path = del_path.pop(-1)
          print(blast_path)
          for i in del_path:
            del_path1 = '/' + i
            delete_path = delete_path + del_path1
          current_avialable_backups = os.listdir(delete_path)
          #print(retention)
          if len(current_avialable_backups) > retention:
            current_avialable_backups.pop(0)
            with open('.dont_delete', 'r') as fin:
              split_data = fin.read().splitlines(True)
            with open('.dont_delete', 'w') as fout:
              fout.writelines(split_data[1:])
          column = "<tr><td>{bkp_path}</td><td>{bkp_size}</td><td>{start_time}</td><td>{end_time}</td><td>{bkp_status}</td><td>{b_type}</td></tr>\n"
          with open('.delete','w') as f:
            f.write(column.format(bkp_path=blast_path,bkp_size=bd_size,start_time=b_strt,end_time=b_end,bkp_status='completed',b_type='full'))
          os.system('cat .delete | cat >> .dont_delete')
          read_file = open('.dont_delete','r').read()
          f_msg = html_message.format(css=css,client=client_name, directory=delete_path,retention=retention,mail_column=read_file)
          #"{}\nBackup path : {}\nBackup DB size : {}\n{}\ncurrent_avialable_backups : {}".format(start_backup,path,bd_size,b_comp1,current_avialable_backups)
          send_mail(f_msg)
          logging.info("BACKUP ENDED : %s\n"%(b_end))
          path = str(path)
          if rscp != None:
            if rscp == 'y' or rscp == 'Y':
              cp_backup_into_server(path,jc_file=jc_file)
          #print(type(path))
          with open('/tmp/mongo_backup_time.txt','a') as f1:
            f1.write('\nBackup started :%s\n' %(b_strt))
            f1.write('\nBackup ended : %s\n' %(b_end))
        else:
          error_4 = os.system('tail -3 %s'%(logfile))
          print error_4
          send_mail(error_4)
          sys.exit()       
    if db_small != 'all':
      DB = db_name
      #print(DB,"---------",collection)
      collss_size = db_coll_size(client,db_name,collection=collection)
      #print(collss_size,"------")
      if collection == None:
        coll_name = raw_input('Type collection name (ALL/coll_name) : ')
      else:
        coll_name = collection
      coll_small = coll_name.lower()
      #print(DB,coll_small)   
  #  if coll_small == 'exit':
  #    sys.exit() 
  if db_small != 'all' and coll_small == 'all':
    DB = db_name
    coll_comp_size = comp_coll_size(client,DB)
    if a_m_m >= coll_comp_size:
      path = mk_dir(backup_path)
      b_strt=print_time()
      logging.info("BACKUP STARTED : %s"%(b_strt))
      backup_start1= '\nBackup started : %s\n' %(b_strt)
      if compression == None:
        comprs = raw_input("Do you want to compressed backup(Y/N) : ")
      else:
        comprs = compression
      if comprs == 'n' or comprs == 'N':
        print(backup_start1)
        process_status = taking_backup(host, port, username, password_ , path, DB,backup_type=backup_type,feildnames=feildnames,logfile=logfile)
      else:
        print(backup_start1)
        process_status = taking_backup(host, port, username, password_ , path, DB,comprs='yes',backup_type=backup_type,feildnames=feildnames,logfile=logfile)
      b_end=print_time()
      print process_status
    #  print process_
      if process_status != 0:
        logging.info("BACKUP ENDED : %s\n"%(b_end))
        b_comp2= '\nBackup Succesfully completed : %s\n' %(b_end)
        print(b_comp2)
        bd_size = commands.getoutput('du -sh {}'.format(path)).split('\t')[0]
        del_path = path
        delete_path = ''
        del_path = del_path.split('/')
        del_path.pop(0)
        blast_path = del_path.pop(-1)
        for i in del_path:
          del_path = '/' + i
          delete_path = delete_path + del_path
        current_avialable_backups = os.listdir(delete_path)
        if len(current_avialable_backups) > retention:
          current_avialable_backups.pop(0)
          with open('.dont_delete', 'r') as fin:
            split_data = fin.read().splitlines(True)
          with open('.dont_delete', 'w') as fout:
            fout.writelines(split_data[1:])
        column = "<tr><td>{bkp_path}</td><td>{bkp_size}</td><td>{start_time}</td><td>{end_time}</td><td>{bkp_status}</td><td>{b_type}</td></tr>\n"
        with open('.delete','w') as f:
          f.write(column.format(bkp_path=blast_path,bkp_size=bd_size,start_time=b_strt,end_time=b_end,bkp_status='completed',b_type='full'))
        os.system('cat .delete | cat >> .dont_delete')
        read_file = open('.dont_delete','r').read()
        f_msg1 = html_message.format(css=css,client=client_name, directory=delete_path,retention=retention,mail_column=read_file)
        #f_msg1 = "{}\nBackup path : {}\nBackup DB size : {}\n{}\ncurrent_avialable_backups : {}".format(backup_start1,path,bd_size,b_comp2,current_avialable_backups)
        send_mail(f_msg1)
        if rscp != None:
          if rscp == 'y' or rscp == 'Y':
            cp_backup_into_server(path,jc_file=jc_file)
        with open('/tmp/mongo_backup_time.txt','a') as f1:
          f1.write('\nBackup started :%s\n' %(b_strt))
          f1.write('\nBackup ended : %s\n' %(b_end))
      else:
        error_5 = os.system('tail -3 %s'%(logfile))
        print error_5
        send_mail(error_5)
        sys.exit()
  if db_small != 'all' and coll_small != 'all':
    DB = db_name
    COLL = coll_name
    coll_comp_size = comp_coll_size(client,DB,COLL)
    if a_m_m >= coll_comp_size:
      path = mk_dir(backup_path)
      b_strt=print_time()
      logging.info("BACKUP STARTED : %s"%(b_strt))
      backup_start2= '\nBackup started : %s\n' %(b_strt)
      if compression == None:
        comprs = raw_input("Do you want to compressed backup(Y/N) : ")
      else:
        comprs = compression
      if comprs == 'n' or comprs == 'N':
        print(backup_start2)
        process_status = taking_backup(host, port, username, password_ , path, DB, COLL,backup_type=backup_type,feildnames=feildnames,logfile=logfile)
      else:
        print(backup_start2)
        process_status = taking_backup(host, port, username, password_ , path, DB, COLL,comprs='yes',backup_type=backup_type,feildnames=feildnames,logfile=logfile) 
      b_end=print_time()
      print process_status
    #  print process_
      if process_status != 0:
        logging.info("BACKUP ENDED : %s\n"%(b_end))
        b_comp3= '\nBackup Succesfully completed : %s\n' %(b_end)
        print(b_comp3)
        bd_size = commands.getoutput('du -sh {}'.format(path)).split('\t')[0]
        del_path = path
        delete_path = ''
        del_path = del_path.split('/')
        del_path.pop(0)
        blast_path = del_path.pop(-1)
        for i in del_path:
          del_path = '/' + i
          delete_path = delete_path + del_path
        current_avialable_backups = os.listdir(delete_path)
        if len(current_avialable_backups) > retention:
          current_avialable_backups.pop(0)
          with open('.dont_delete', 'r') as fin:
            split_data = fin.read().splitlines(True)
          with open('.dont_delete', 'w') as fout:
            fout.writelines(split_data[1:])
        column = "<tr><td>{bkp_path}</td><td>{bkp_size}</td><td>{start_time}</td><td>{end_time}</td><td>{bkp_status}</td><td>{b_type}</td></tr>\n"
        with open('.delete','w') as f:
          f.write(column.format(bkp_path=blast_path,bkp_size=bd_size,start_time=b_strt,end_time=b_end,bkp_status='completed',b_type='full'))
        os.system('cat .delete | cat >> .dont_delete')
        read_file = open('.dont_delete','r').read()
        f_msg2 = html_message.format(css=css,client=client_name, directory=delete_path,retention=retention,mail_column=read_file)
        #f_msg2 = "{}\nBackup path : {}\nBackup DB size : {}\n{}\ncurrent_avialable_backups : {}".format(backup_start2,path,bd_size,b_comp3,current_avialable_backups)
        send_mail(f_msg2)
        if rscp != None:
          if rscp == 'y' or rscp == 'Y':
            cp_backup_into_server(path,jc_file=jc_file)
        with open('/tmp/mongo_backup_time.txt','a') as f1:
          f1.write('\nBackup started :%s\n' %(b_strt))
          f1.write('\nBackup ended : %s\n' %(b_end))
      else:
        error_6 = os.system('tail -3 %s'%(logfile))
        print error_6
        send_mail(error_6)
        sys.exit()
  return path    #time.sleep(0.5)


if retention == None:
  try:
    take_backup(db,collection,compression,backup_path,retention=retention,backup_type=backup_type,feildnames=feildnames,client_name=client_name,logfile=logfile)
  except:
    error_ = commands.getoutput('grep Failed temp')
    error_ = error_ + "\nBackup failed"
    print("ERROR : %s"%(error_))
    logging.error("ERROR : %s"%(error_))
    send_mail(error_)
    sys.exit()
else:
  try:
    del_path = take_backup(db,collection,compression,backup_path,retention=retention,backup_type=backup_type,feildnames=feildnames,client_name=client_name,logfile=logfile)
    delete_path = ''
    del_path = del_path.split('/')
    del_path.pop(0)
    del_path.pop(-1)
    for i in del_path:
      del_path = '/' + i
      delete_path = delete_path + del_path
    delete_path_list = os.listdir(delete_path)
    dep_path = os.path.join(os.getcwd(),'take_backup.py')
    deploycron(content='0 0 * * * python {} -f {}'.format(dep_path,jc_file))
    if len(delete_path_list) > retention:
      rm_path = delete_path+'/'+delete_path_list[0]
      print("removed : " + rm_path)
      shutil.rmtree(rm_path)
    #print(delete_path)
  except:
    error_ = commands.getoutput('grep Failed temp')
    error_ = error_ + "\nBackup failed"
    print("ERROR : %s"%(error_))
    logging.error("ERROR : %s"%(error_))
    send_mail(error_)
    sys.exit()

