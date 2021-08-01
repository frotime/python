#유튜브
from googleapiclient.discovery import build

#구글스프레드시트
from oauth2client.service_account import ServiceAccountCredentials
import gspread

import pandas
import datetime

youtube = build('youtube', 'v3',
  developerKey = "AIzaSyCpXPMF4Y6rtjSgM4Yx4m4sgXPi7KBibns")

def getSubscriberCount(channel_id):
  channels_response = youtube.channels().list(part = "statistics", id = channel_id).execute()
  statistics = channels_response["items"][0]['statistics']
  if 'subscriberCount' in statistics:
    subscriberCount = channels_response["items"][0]['statistics']['subscriberCount'] 
  else:
    subscriberCount = 0
  return subscriberCount

def getChannelName(channel_id):
  channels_response = youtube.channels().list(part = "snippet", id = channel_id).execute()
  channel_name = channels_response["items"][0]['snippet']['title']      
  return channel_name  

def getVideoViewCount(video_id):
  view_count = youtube.videos().list(id = video_id, part = "statistics").execute()['items'][0]['statistics']['viewCount']
  return view_count

def getChannelUrl(channel_id):
  channel_url = "https://www.youtube.com/channel/"+channel_id
  return channel_url;

def getVideoUrl(video_id):
  video_url = "https://www.youtube.com/watch?v="+video_id
  return video_url



scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
        '/Users/juryeonkim/Downloads/client_secret.json', scope)
gc = gspread.authorize(credentials)

channel_sheet = gc.open("python_test").worksheet('채널')
channel_df = pandas.DataFrame(channel_sheet.get_all_records())
# 채널 아이디값 목록개수를 확인해서 루프를 돈다.
for row in channel_df.itertuples():
  channel_df.at[row.Index, '채널명'] = getChannelName(row.채널아이디)
  channel_df.at[row.Index, '구독자수'] = getSubscriberCount(row.채널아이디)
  channel_df.at[row.Index, '채널주소'] = getChannelUrl(row.채널아이디)

# 채널정보 업데이트
channel_sheet.update(([channel_df.columns.values.tolist()] + channel_df.values.tolist()))

# 채널 아이디값을 가지고 업로드된 동영상의 정보를 업데이트한다.
column_names = ["썸네일", "조회수", "초과조회수", "타이틀", "url", "등록일", "채널명", "구독자수"]
video_df = pandas.DataFrame(columns = column_names)
video_sheet = gc.open("python_test").worksheet('동영상')

for row in channel_df.itertuples():
  channel_id = row.채널아이디
  subscribers_count = int(row.구독자수)
  channel_name = row.채널명
  print("채널 동영상 검색 시작")
  print(datetime.datetime.now())
  # 동영상 목록 구하기
  channels_response = youtube.channels().list(part = "contentDetails", id = channel_id).execute()
  all_playlist_id = channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  playlistitems_list_request = youtube.playlistItems().list(part = "snippet", playlistId = all_playlist_id, maxResults=50)

  while playlistitems_list_request:
    playlistitems_list_response = playlistitems_list_request.execute()
    for playlist_item in playlistitems_list_response["items"]:
      title = playlist_item["snippet"]["title"]
      published_at = playlist_item["snippet"]["publishedAt"]
      description = playlist_item["snippet"]["description"]
      thumbnail_url = playlist_item["snippet"]["thumbnails"]["default"]["url"]
      video_id = playlist_item["snippet"]['resourceId']['videoId']
      video_url = getVideoUrl(video_id)
      view_count = int(getVideoViewCount(video_id))
      over_view_count = view_count - subscribers_count
      video_df=video_df.append({'썸네일' : thumbnail_url , '조회수' : view_count, '초과조회수' : over_view_count, '타이틀' : title , 'url' : video_url, '등록일' : published_at , '채널명' : channel_name, '구독자수' : subscribers_count} , ignore_index=True)
    playlistitems_list_request = youtube.playlistItems().list_next(playlistitems_list_request, playlistitems_list_response)
  print("채널 동영상 검색 끝")
  print(datetime.datetime.now())
  
video_sheet.update(([video_df.columns.values.tolist()] + video_df.values.tolist()))

