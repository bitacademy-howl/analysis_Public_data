import collect_from_webapi

# test for pd_gen_url
url = collect_from_webapi.api.pd_gen_url(YM='{0:04d}{1:02d}'.format(2017, 1),
    SIDO='서울특별시',
    GUNGU='',
    RES_NM='',
    numOfRows=10,
    _type='json',
    pageNo=1)

print(url)


