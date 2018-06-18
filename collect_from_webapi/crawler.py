import os
import json

from collect_from_webapi.api_public_data import pd_fetch_tourspot_visitor

RESULT_DIRECTORY = '__result__/crawling'

def preprocess_post(post):

        post['count_locals'] = post.pop('csNatCnt')
        post['count_foreigner'] = post.pop('csForCnt')
        post['tourist_spot'] = post.pop('resNm')
        post['date'] = post.pop('ym')
        post['city'] = post.pop('sido')
        post['restrict'] = post.pop('gungu')
        del post["rnum"]
        del post["addrCd"]

def crawlling_tourspot_visitor(district, start_year, end_year):

    filename = '%s/%s_tourinstspot_%s_%s.json' % (RESULT_DIRECTORY, district, start_year, end_year)

    # 문제 :
    # result를 크롤링 함수의 지역변수로 지정하게 되면(Version 1의 경우)
    # 크롤링 중 메모리에 모든 자료를 다 올리게 되고, 방대한 데이터를 크롤링 하는 경우 버퍼(or스택) 오버플로우 발생 가능
    # 일련의 단위별로 저장하는 방법이 필요
    # step 1:
    # 아래서는 파일을 함수내에서 열어서 쓰지만
    # 프로그램 리셋 시 이전에 기록한 데이터가 지워지는 단점
    # DB 엑세싱이라면 값을 테이블에 추가하고 데이터가 유지되지만
    # 아래와 같이 파일 입출력의 경우
    # step 2:
    # 기존에 있는 파일에 이어서 데이터를 기록하여야 함
    # step 3: DB 엑세싱에 관한 방법도 찾아볼 것!
    with open(filename, 'w', encoding='utf-8') as outfile:
        # 1년 12달, 음력/윤년 고려 안함
        for searchingyear in range(start_year, end_year+1):
            for searchingmonth in range(1, 13):
                for posts in pd_fetch_tourspot_visitor(district, year=searchingyear, month=searchingmonth):
                    for post in posts:
                        preprocess_post(post)
                    # save results to file (저장/적재)
                    json_String = json.dumps(posts, indent=4, sort_keys=True, ensure_ascii=False)
                    outfile.write(json_String)

if os.path.exists(RESULT_DIRECTORY) is False:
    os.makedirs(RESULT_DIRECTORY)