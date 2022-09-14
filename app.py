
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS, cross_origin
from selenium import webdriver
from selenium.webdriver.common.by import By
from pytube import YouTube
import time
import pandas as pd
import requests
import snowflake.connector
import pymongo
import boto3
import os
import shutil
from snowflake.connector.pandas_tools import write_pandas


client = pymongo.MongoClient("mongodb+srv://" + str(os.environ.get('mongo_user')) + ":" + str(os.environ.get('mongo_password')) + "@clusterp.tlvbf.mongodb.net/?retryWrites=true&w=majority")

print(client)

ctx = snowflake.connector.connect(
    user=os.environ.get('snowflake_user'),
    password=os.environ.get('snowflake_password'),
    account=os.environ.get('snowflake_account'),
    region=os.environ.get('snowflake_region'))
print(ctx)

s3 = boto3.resource(
    service_name='s3',
    aws_access_key_id=str(os.environ.get('aws_access')),
    aws_secret_access_key=str(os.environ.get('aws_secret_key')))
print(s3)




def get_vedios_list(url,wd,num):
    '''This function is used to get the list of the required vedios and details of youtube user'''
    try:
        # url = 'https://www.youtube.com/user/krishnaik06/videos'
        wd.get(url)
        video_sections = []
        while len(video_sections) < num:
            wd.execute_script("window.scrollBy(0,100)", "")
            video_sections = wd.find_elements(By.ID, "video-title")

        vedio_details_list = []
        for i in video_sections[0:num]:
            link = i.get_attribute('href')
            yt1 = YouTube(link)
            details = {
                "channel_link": url,
                "vedio_link": link,
                "vedio_title": yt1.title,
                "vedio_thumbnail_url": yt1.thumbnail_url,
                "vedio_views": yt1.views,
                "vedio_description": yt1.description,
                "vedio_id": link.split('=')[-1]

            }
            #print(details)
            vedio_details_list.append(details)
        df = pd.DataFrame(vedio_details_list[0:num], columns=['vedio_id', 'vedio_title', 'vedio_link', 'vedio_views',
                                                              'vedio_thumbnail_url', 'vedio_description',
                                                              'channel_link'])

        wd.find_element(By.XPATH, '//*[@id="tabsContent"]/tp-yt-paper-tab[6]').click()
        time.sleep(5)
        user = wd.find_elements(By.XPATH, '//*[@id="meta"]')[0].text
        user_details_dict = {
            'channel_link': url,
            'channel_name': user.split('\n')[0],
            'subscribers': user.split('\n')[1].split(' ')[0],
            'channel_description': wd.find_elements(By.XPATH, '//*[@id="description-container"]')[0].text
        }


        return [df,user_details_dict]

    except Exception as e:
        print("something went wrong while fetching vedios details: "+str(e))

def comment_likes(vedio_link, wd):
    '''This function is used to open url of each vedio and scrap the likes and coments of the vedio
    and update them in the snowflake and mongodb'''
    try:
        wd.get(vedio_link)
        wd.execute_script("window.scrollBy(0,800)", "")
        time.sleep(30)
        comments = wd.find_elements(By.XPATH, '//*[@id="contents"]/ytd-comment-thread-renderer')
        while len(comments) == 0:
            wd.execute_script("window.scrollBy(0,100)", "")
            time.sleep(10)
            comments = wd.find_elements(By.XPATH, '//*[@id="contents"]/ytd-comment-thread-renderer')

        cmnt_list = []
        for i in comments:
            name = i.find_element(By.ID, 'header-author').text.split('\n')[0]
            c = i.find_element(By.ID, "comment-content").text
            cm = {'Name': name,
                  'Comment': c}
            cmnt_list.append(cm)
            time.sleep(2)
        #print(cmnt_list)
        vedio_rating = wd.find_elements(By.XPATH, '//*[@id="top-level-buttons-computed"]/ytd-toggle-button-renderer[1]/a')[0].text
        df1 = pd.DataFrame(cmnt_list, columns=['Name', 'Comment'])
        #cs = ctx.cursor()
        #cs.execute('use database1')
        #cs.execute(
        #    "update VEDIO_DETAILS_LIST set VEDIO_LIKES='" + vedio_rating + "' where vedio_link='" + vedio_link + "' ")
        # return [df1,vedio_rating]
        #print(df1)
        return([cmnt_list,vedio_rating])


    except Exception as e:
        print('something went wrong  ' + str(e))

def vedio_storage(videourl, path, bucket_name, s3):
    '''This function would download the vedio and upload it ot the s3 bucket'''
    try:

        yt1 = YouTube(videourl)
        yt1 = yt1.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
        if path == '': path = './scrap_vedios'
        if not os.path.exists(path):
            os.makedirs(path)
        x = yt1.download(path)
        print(x)
        print('vedio downloaded successfully')

        bucket = []
        response = s3.create_bucket(Bucket=bucket_name)

        existingvedios = []
        [existingvedios.append((file.key)) for file in s3.Bucket(bucket_name).objects.all()]

        # check if the vedio already exists in the bucket
        key1 = x.split('\\')[-1]
        file_path = x.replace('./', '/').replace('\\', '/').replace('//', '/')
        if key1 in existingvedios:
            print('vedio already exists')
        else:
            s3.Bucket(bucket_name).upload_file(file_path, Key=key1)

        if os.path.exists(path):
            print("deleting vedios path: " + path)
            shutil.rmtree(path)
        return (['vedio uploaded to s3 successfully to', file_path])


    except Exception as e:
        print("Error while uploading the vedio to s3 " + str(e))


def load_vediolistdetails(df2, user_dict,ctx, client):
    '''This function will load the details about the required number of vedios in to the table
    in snowflake and load vedio_description, image in to the MongoDB with their object_id's stored
    in the table. call once'''
    try:

        df2['thumbnail_image'] = df2['vedio_thumbnail_url'].apply(lambda i: requests.get(i).content)
        #print(df2['thumbnail_image'])
        df1 = df2.copy()
        #print(df1.columns)
        df1 = df1[['vedio_link', 'vedio_description', 'thumbnail_image']]
        #print(df1)
        database1 = client['database1']
        collection01 = database1['youtuber_description']
        collection01.delete_many({'channel_link': user_dict['channel_link']})
        collection01.insert_one(
            {'channel_link': user_dict['channel_link'], 'channel_description': user_dict['channel_description']})
        rec1 = collection01.find({'channel_link': user_dict['channel_link']}, {'channel_link': 1})
        j1 = list(rec1)
        rec = []
        [rec.append(j) for j in j1]
        print(rec)
        collection = database1['vedios_description']
        collection.delete_many({'vedio_link': {'$in': df1['vedio_link'].to_list()}})
        collection.insert_many(df1.apply(lambda x: x.to_dict(), axis=1).to_list())
        print(df1['vedio_link'].to_list())
        record = collection.find({'vedio_link': {'$in': df1['vedio_link'].to_list()}}, {'vedio_link': 1})
        x = list(record)
        print(x)
        x1 = []
        [x1.append(j) for j in x]
        print(x1)
        df2['details_mongoid'] = df2['vedio_link'].apply(lambda a: [str(i['_id']) for i in x1 if i['vedio_link'] == a])
        #print(df2['details_mongoid'])

        # return (pd.DataFrame(sn_res, columns=col))
    except Exception as e:
        print('something went wrong while updating' + str(e))
    try:
        if len(df1) == 0:
            raise Exception("length of dataframe received is 0, please verify")
        df_in = df2[['vedio_id', 'vedio_title', 'vedio_link', 'vedio_views', 'vedio_likes', 'vedio_thumbnail_url',
                     'details_mongoid', 'channel_link']]
        df_in.columns = df_in.columns.str.upper()
        cs = ctx.cursor()
        cs.execute('use database1')
        cs.execute(
            'create table if not exists youtubeusers(channel_link varchar(500),channel_name varchar(100),subscribers varchar(30) , channel_description_id varchar(2000), PRIMARY KEY (channel_link))')
        cs.execute('select channel_link from youtubeusers')
        res1 = [i[0] for i in cs.fetchall()]
        #print(res1)
        if user_dict['subscribers'] in res1:
            print('url already exists, updating details')
        else:
            cs.execute("insert into youtubeusers values('" + user_dict['subscribers'] + "',null,null,null)")

        cs.execute("update youtubeusers set channel_description_id='" + str(rec[0]['_id']) + "', channel_name='" +
                   user_dict['channel_name'] + "' ,subscribers='" + user_dict['subscribers']
                   + "' where channel_link='" + user_dict['channel_link'] + "'")

        cs.execute('''create table if not exists VEDIO_DETAILS_LIST	(VEDIO_ID VARCHAR(100),
                VEDIO_TITLE VARCHAR(1000),VEDIO_LINK VARCHAR(1000),VEDIO_VIEWS VARCHAR(100),
                VEDIO_LIKES VARCHAR(100),VEDIO_THUMBNAIL_URL VARCHAR(1000),DETAILS_MONGOID VARCHAR(500),
                CHANNEL_LINK VARCHAR(500),constraint FK_CHANNEL_LINK foreign key (CHANNEL_LINK) 
                references DATABASE1.PUBLIC.YOUTUBEUSERS(CHANNEL_LINK));''')
        cs.execute('select vedio_link from vedio_details_list')
        res2 = [i[0] for i in cs.fetchall()]
        for i in res2:
            if i in df_in['VEDIO_LINK'].to_list():
                cs.execute("delete from vedio_details_list where vedio_link='" + i + "' and channel_link='" + user_dict[
                    'channel_link'] + "'")

        # df.to_sql(name='vedio_details_list',con=ctx,if_exists='append',index=False,method=pd_writer))
        TBLE = 'VEDIO_DETAILS_LIST'
        try:
            chunks, nrows, _ = write_pandas(ctx, df_in, TBLE.upper(), quote_identifiers=False)
        except Exception as e:
            print(e)

        return df2

    except Exception as e:
            print('something went wrong while updating' + str(e))

def read_comment(vedio_link, client):
    print(vedio_link)
    database1 = client['database1']
    collection02 = database1['comments']
    record = collection02.find({'vedio_link': vedio_link})
    rec = []
    [rec.append(j) for j in record]
    return rec[0]['comment_list']

app = Flask(__name__)


@app.route('/', methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")


@app.route('/comntss', methods=['POST', 'GET'])  # route to show the review comments in a web UI
@cross_origin()
def comntss():
    if request.method == 'POST':
        try:
            comm_link = request.form['commnt'].replace(" ", "")
            print(comm_link)
            commentss = (read_comment(comm_link, client))
            print(commentss)
            return render_template('results1.html', commentss=commentss)
        # print(read_comment(request.form.get("comment"),client))
        except Exception as e:
            print('The Exception message is: ', e)
            return 'something is wrong'

    else:
        return render_template('index.html')


@app.route('/review', methods=['POST', 'GET'])  # route to show the review comments in a web UI
@cross_origin()
def index():
    if request.method == 'POST':
        try:
            url1 = request.form['content'].replace(" ", "")
            num = request.form['num']
            #ld = request.form['ld'].replace(" ", "").upper()
            numd = request.form['numd']
            num = int(num)
            if numd == '':
                numd = 0
            else:
                numd = int(numd)
            
            chrome_options = webdriver.ChromeOptions()
            chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")

            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")

            wd = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=chrome_options)
            
            #url1 = 'https://www.youtube.com/user/krishnaik06/videos'
            #wd = webdriver.Chrome("chromedriver.exe")

            l1=[]
            l1=get_vedios_list(url1,wd,num)
            df_vedios=l1[0]
            user_dict=l1[1]
            print(df_vedios)
            list1=[]
            df=df_vedios.copy()
            vedio_lst = df['vedio_link'].to_list()
            for i in vedio_lst:
                l = comment_likes(i, wd)
                #print(l)
                list1.append({'vedio_link': i, 'vedio_likes': l[1]})
                try:
                    database1 = client['database1']
                    collection02 = database1['comments']
                    collection02.delete_many({'vedio_link': i})
                    comnt_dict = {'vedio_link': i,
                                  'comment_list': l[0]}
                    collection02.insert_one(comnt_dict)
                except Exception as e:
                    print(e)

            df2 = pd.DataFrame(list1, columns=['vedio_link', 'vedio_likes',])
            print(df2)
            df3 = pd.DataFrame(pd.merge(df, df2, how='inner'))
            df3
            df=pd.DataFrame(load_vediolistdetails(df3,user_dict,ctx,client))

            #numd=0
            for i in vedio_lst[0:numd]:
                vedio_storage(i,'./scrap_vedios9', "pyvedioscrapping", s3)
            dff=df[['vedio_title','vedio_link','vedio_views','vedio_likes','vedio_thumbnail_url']]
            review1=dff.apply(lambda x: x.to_dict(), axis=1).to_list()
            #print(review1)
            #print(user_dict)
            reviews = [review1, user_dict]
            print(reviews)
            return render_template('results.html', reviews=reviews)

        except Exception as e:
            wd.close()
            print('The Exception message is: ', e)
            return 'something is wrong'

    else:
        return render_template('index.html')


if __name__ == "__main__":
    # app.run(host='127.0.0.1', port=8001, debug=True)
    app.run(debug=True)



