import requests
import time
import random
import json
import sys

token = '' # PUT YOUR TOKEN HERE
if not token:
    print("Please put your token inside the code ex: token = 'Xcad4j23g12ft34k2' \nGo for get token : https://developers.facebook.com/tools/explorer/\n")
    sys.exit()

def fb_req(req):
    r = requests.get("https://graph.facebook.com/v2.12/" + req , {'access_token': token})
    return r


fields = "id,from,permalink_url,likes.summary(true),comments.summary(true),message"
req = "me/posts?fields={fields}".format(fields=fields)


results = fb_req(req).json()

data = []
#results = results['posts']
i = 0
while True:
    try:
        time.sleep(random.randint(2,5))
        data.extend(results['data'])
        r = requests.get(results['paging']['next'])
        results = r.json()
        i += 1
        if i > 5:
            break
    except:
        print("{}".format(results))
        break

final_data = []
fb_url = 'https://www.facebook.com/'
#print(results)
#print(data)
for i in data:
    try:
        post_caption=i['message']
    except:
        post_caption = 'VIDEO/FILE'
    y = i['likes']['data']
    comment_name_msg = {}
    for name in i['comments']['data']:
        comment_name_msg.update({name['from']['name']:{'comment':name['message'],'profile_link': fb_url+name['from']['id']}})
    if not comment_name_msg:
        comment_name_msg='No_comments'
    x = {
    'post_link': i['permalink_url'],
    'post_like_count': i['likes']['summary']['total_count'],
    'post_like_names': [l['name'] for l in y],
    'total_comment_count': i['comments']['summary']['total_count'],
    'post_comments': comment_name_msg,
    'post_caption' : post_caption
    }
    final_data.append(x)
"""
js = json.dumps([{'post_link': datain['post_link'], 
                'post_like_count': datain['post_like_count'],
                'post_like_names': datain['post_like_names'],
                'total_comment_count': datain['total_comment_count'],
                'post_comments':datain['post_comments'],
                'post_caption':datain['post_caption']} for datain in final_data], indent=4)

fp = open('fb.json','w')
fp.write(js)
fp.close() 
"""
with open('data.json', 'w') as outfile:
    json.dump(final_data, outfile, sort_keys = True, indent = 4,ensure_ascii = False)

for i in final_data:
    print("post_link : {}".format(i['post_link']))
    print("post_like_count : {}".format(i['post_like_count']))
    print("post_like_names : {}".format(i['post_like_names']))
    print("total_comment_count : {}".format(i['total_comment_count']))
    print("post_comments : {}".format(i['post_comments']))
    print("post_caption : {}".format(i['post_caption']))
    print("\n-------------------------------------------------------------------\n")

