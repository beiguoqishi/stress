# NCT考试清除
import csv
import time
import pymysql


# 清除考试数据
def clear_eaxm():
    # 创建数据库连接--test数据库
    conn = pymysql.connect(
        host="rm-bp1326x2083c1021v.mysql.rds.aliyuncs.com",
        user='test_all',
        password='Mfi38IHf4hGHGh',
        port=3306,
        database='nct_examination'
    )

    # 创建游标对象
    cursor = conn.cursor()

    # 读取 CSV 文件并准备批量插入数据
    batch_size = 1  # 每次批量插入的行数
    user_id_data = []

    # 创建游标对象
    cursor = conn.cursor()

    # 读取 CSV 文件并准备批量插入数据
    batch_size = 100  # 每次批量插入的行数
    batch_count = 0

    with open('/Users/fanny/Desktop/nct_user_info_user.csv', encoding='utf-8') as f:
        # f.readline()  # 跳过标题行
        line1 = f.readline()

        EXAMINATION_ID = 1488
        records = []
        while line1:
            arr = line1.strip().split(',')
            student_id = arr[0]
            record = (EXAMINATION_ID, student_id)
            records.append(record)
            if len(records) >= batch_size:
                sql1=f"DELETE FROM stud_paper WHERE examination_id=%s AND student_id=%s"
                sql2=f"DELETE FROM stud_paper_question WHERE examination_id=%s AND student_id=%s"
                sql3=f"""
                            UPDATE exam_examination_student
                            SET student_waiting_time=NULL,
                                student_begin_time=NULL,
                                student_submit_time=NULL,
                                student_segment_one_submit_time=NULL,
                                student_auth_times=0,
                                student_invigilate_ai_times=0,
                                student_screen_toggle_times=0,
                                student_monitor_url=null
                            WHERE examination_id=%s AND student_id=%s
                            """

                c1 =cursor.executemany(sql1, records)
                c2 =cursor.executemany(sql2, records)
                c3 =cursor.executemany(sql3, records)
                batch_count += 1
                print(f"batch_count: {batch_count}, c1: {c1}, c2: {c2}, c3: {c3}")
                conn.commit()
                records = []  # 清空数据列表

            line1 = f.readline()

if __name__ == '__main__':
    clear_eaxm()