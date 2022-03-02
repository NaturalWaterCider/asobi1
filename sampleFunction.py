
import json
import boto3
import io
import pandas as pd
import random

# 毎回csv読み込むのもあほらしくない？とやや思ふ

# バケット名,オブジェクト名
BUCKET_NAME = 'asobi1-stations'
LINE_OBJECT_KEY_NAME = 'line20210312free.csv'
STATION_OBJECT_KEY_NAME = 'station20210312free.csv'
SRC_FILE_ENCODING="utf-8"

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    # TODO implement
    # オブジェクト取得
    line_obj = s3.Object(BUCKET_NAME, LINE_OBJECT_KEY_NAME)
    station_obj = s3.Object(BUCKET_NAME, STATION_OBJECT_KEY_NAME)

    # ファイル取得 & 読み込み
    line_body = line_obj.get()['Body'].read().decode(SRC_FILE_ENCODING)
    station_body = station_obj.get()['Body'].read().decode(SRC_FILE_ENCODING)
    
    # CSVテキストをバッファに書き出し
    line_buffer = io.StringIO(line_body)
    station_buffer = io.StringIO(station_body)
    
    # 路線データ取得
    lines = pd.read_csv(line_buffer)
    # 駅データ取得
    stations = pd.read_csv(station_buffer)
    
    #ほしい路線の路線コードだけとってくる
    lines = lines[lines.line_name.str.contains('JR山手線|東京メトロ|都営|都電')]
    lines = lines['line_cd']
    
    # ほしい路線の駅だけとってくる
    stations = stations.query("line_cd in @lines")
    
    # 重複をなくす(いちがやは諦める)
    stations = stations['station_name'].unique()
    
    # 目的地を選ぶ
    dest = random.choice(stations)
    
    # 返す
    return {
        'statusCode': 200,
        'body': json.dumps(dest, ensure_ascii=False),
        'headers': {
            "Access-Control-Allow-Origin": "*"
        }
    }
