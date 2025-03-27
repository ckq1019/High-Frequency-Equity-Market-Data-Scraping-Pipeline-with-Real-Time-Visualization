import time
import datetime
import pandas as pd
import oracledb
import logging

# 配置日志
logging.basicConfig(
    filename='app.log',  # 日志文件名
    level=logging.ERROR,  # 记录错误及以上级别的日志
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def capture_stock_chart():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    url = 'https://so.eastmoney.com/web/s?keyword=932056'  # 对应网页
    try:
        driver.get(url)
        time.sleep(5)
        element = driver.find_element(By.CLASS_NAME, 'charts_c')
        file_name = datetime.datetime.now().strftime("%Y%m%d%H%M")
        element.screenshot('{}.png'.format(file_name))
        print('{}.png save success.'.format(file_name))
    except Exception as e:
        print(f'ERROR: {e}')
    finally:
        driver.quit()


def insert_dataframe_to_db(df, table_name, column_map, pool):
    original_columns = set(column_map.keys())
    if not original_columns.issubset(df.columns):
        missing = original_columns - set(df.columns)
        raise ValueError(f"DataFrame 缺少以下列: {missing}")

    target_columns = list(column_map.values())

    placeholders = ', '.join([f":{col}" for col in target_columns])
    sql = f"INSERT INTO {table_name} ({', '.join(target_columns)}) VALUES ({placeholders})"
    try:
        with pool.acquire() as connection:
            with connection.cursor() as cursor:
                for index, row in df.iterrows():
                    try:
                        params = {source: row[target] for target, source in column_map.items()}
                        print(params)
                        cursor.execute(sql, params)
                        if index % 1000 == 0:
                            connection.commit()
                            print(f"已插入 {index} 条记录")
                    except oracledb.DatabaseError as e:
                        error, = e.args
                        print(f"第 {index} 条记录插入失败: {error.message}")
                connection.commit()
                print(f"成功插入总共 {index + 1} 条记录到表 {table_name}")
    except Exception as e:
        logging.error(f"数据库插入失败: {e}")
        raise


def capture_stock_history():
    try:
        import akshare as ak
        today = datetime.datetime.today().strftime("%Y%m%d")
        # df = ak.index_zh_a_hist(symbol="932056", period="daily", start_date="20200101", end_date=today)
        df = ak.index_zh_a_hist(symbol="932056", period="daily", start_date=today, end_date=today)
        print(df)
        # csv_file_path = '{}.csv'.format(today)
        # df.to_csv(csv_file_path, index=False)
        # 存入数据库中
        user = "hy"
        password = "hy"
        host = "31.16.1.83"
        port = 1521
        service_name = "jiaohuan"
        dsn = oracledb.makedsn(host, port, service_name=service_name)
        oracledb.init_oracle_client(lib_dir=r"instantclient-basic-windows.x64-19.26.0.0.0dbru\instantclient_19_26")
        pool = oracledb.create_pool(user=user, password=password, dsn=dsn, min=2, max=5, increment=1)
        df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d')
        column_mapping = {
            '日期': 'DT_TIME',
            '开盘': 'NM_OPEN',
            '收盘': 'NM_CLOSE',
            '最高': 'NM_HIGH',
            '最低': 'NM_LOW',
            '成交量': 'NM_VOL',
            '成交额': 'NM_AMT',
            '振幅': 'NM_AMP',
            '涨跌幅': 'NM_PCTCHG',
            '涨跌额': 'NM_CHG',
            '换手率': 'NM_TR'
        }
        insert_dataframe_to_db(df, 'HY.TB_HY_ECONOMY', column_mapping, pool)
        pool.close()
    except Exception as e:
        logging.error(f"获取股票历史数据失败: {e}")
        raise


def job():
    print(f"{datetime.datetime.now()}，start job...")
    try:
        # 获取历史数据
        capture_stock_history()
        # 获取当天走势图
        # capture_stock_chart()
    except Exception as e:
        logging.error(f"定时任务执行失败: {e}")


if __name__ == "__main__":
    # job()
    import schedule
    for day in ['mon', 'tue', 'wed', 'thu', 'fri']:
        schedule.every().monday.at("15:30").do(job) if day == 'mon' else \
            schedule.every().tuesday.at("15:30").do(job) if day == 'tue' else \
                schedule.every().wednesday.at("15:30").do(job) if day == 'wed' else \
                    schedule.every().thursday.at("15:30").do(job) if day == 'thu' else \
                        schedule.every().friday.at("17:30").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
