import pandas as pd
import os

def get_dataset(path, chunk_size, nums_row):
    # 定义数据存储列表
    data_list = []
    chunks_read = 0
    num_rows_to_read = nums_row

    # 逐块加载JSON数据
    chunk_size = chunk_size  # 根据需要调整块大小
    with open(path, 'r', encoding='utf-8') as file:
        for chunk in pd.read_json(file, lines=True, orient='records', chunksize=chunk_size):
            session_id = chunk['session'].iloc[0]
            events = chunk['events']
            for event in events:
                for info in event:
                    aid = info['aid']
                    ts = info['ts']
                    type = info['type']
                    data_list.append([session_id, aid, ts, type])
            chunks_read += len(chunk)

            # 检查是否已经读取足够的行数
            if chunks_read >= num_rows_to_read:
                break

    # 合并所有块成一个DataFrame
    columns = ['session_id', 'aid', 'ts', 'event_type']
    df = pd.DataFrame(data_list, columns=columns)
    return df

def get_indice(topn=20, ):

    indices = 0
    return indices


