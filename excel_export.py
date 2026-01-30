import pandas as pd
import sqlite3
import os
from datetime import datetime

DB_PATH = 'data/ppm.db'
OUTPUT_DIR = 'output'
DATA_DIR = 'data'

# Part별 공정 정의
PART_PROCESSES = {
    '1Part': ['성형', '소결', '정형', '압입', '가공', '밴딩', '후처리'],
    '2Part': ['성형', '소결', '정형', '후처리']
}

# Part별 템플릿 파일
TEMPLATE_FILES = {
    '1Part': 'Part1_20260130.xlsx',
    '2Part': 'Part2_20260130.xlsx'
}


def get_defect_columns(part_type):
    """Part별 불량 컬럼 목록 조회"""
    template_file = os.path.join(DATA_DIR, TEMPLATE_FILES[part_type])
    xlsx = pd.ExcelFile(template_file)
    df = pd.read_excel(xlsx, sheet_name=xlsx.sheet_names[0])

    # code, TM_No, 품명, 합계를 제외한 나머지가 불량 컬럼
    exclude_columns = ['code', 'TM_No', '품명', '합계']
    defect_columns = [col for col in df.columns if col not in exclude_columns]

    return defect_columns


def get_all_columns(part_type):
    """Part별 전체 컬럼 목록 (템플릿 기준)"""
    template_file = os.path.join(DATA_DIR, TEMPLATE_FILES[part_type])
    xlsx = pd.ExcelFile(template_file)
    df = pd.read_excel(xlsx, sheet_name=xlsx.sheet_names[0])
    return list(df.columns)


def export_daily_excel(record_date, part_type):
    """일별 Excel 파일 생성"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    processes = PART_PROCESSES.get(part_type, [])
    all_columns = get_all_columns(part_type)
    defect_columns = get_defect_columns(part_type)

    # 파일명 생성 (Part1_20260130.xlsx 형식)
    date_str = record_date.replace('-', '')
    part_num = '1' if part_type == '1Part' else '2'
    filename = f'Part{part_num}_{date_str}.xlsx'
    filepath = os.path.join(OUTPUT_DIR, filename)

    # Excel Writer 생성
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        for process_name in processes:
            # 해당 공정의 데이터 조회
            cursor = conn.cursor()
            cursor.execute('''
                SELECT tm_no, code, 품명, defect_name, quantity
                FROM defect_records
                WHERE record_date = ? AND part_type = ? AND process_name = ?
                ORDER BY tm_no
            ''', (record_date, part_type, process_name))

            records = cursor.fetchall()

            # 데이터를 피벗 형태로 변환
            data_dict = {}
            for row in records:
                tm_no = row['tm_no']
                if tm_no not in data_dict:
                    data_dict[tm_no] = {
                        'code': row['code'],
                        'TM_No': tm_no,
                        '품명': row['품명'],
                        '합계': 0
                    }
                    # 불량 컬럼 초기화
                    for col in defect_columns:
                        data_dict[tm_no][col] = 0

                defect_name = row['defect_name']
                quantity = row['quantity'] or 0
                if defect_name in data_dict[tm_no]:
                    data_dict[tm_no][defect_name] = quantity
                    data_dict[tm_no]['합계'] += quantity

            # DataFrame 생성
            if data_dict:
                df = pd.DataFrame(list(data_dict.values()))
                # 컬럼 순서 맞추기
                df = df.reindex(columns=all_columns, fill_value=0)
            else:
                # 빈 DataFrame (컬럼만 있는)
                df = pd.DataFrame(columns=all_columns)

            # 시트에 저장
            df.to_excel(writer, sheet_name=process_name, index=False)

    conn.close()
    return filename


def get_daily_summary(record_date, part_type):
    """일별 요약 데이터 조회"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT process_name, COUNT(DISTINCT tm_no) as item_count, SUM(quantity) as total_defects
        FROM defect_records
        WHERE record_date = ? AND part_type = ?
        GROUP BY process_name
    ''', (record_date, part_type))

    results = cursor.fetchall()
    conn.close()

    return [{'process': row[0], 'items': row[1], 'defects': row[2]} for row in results]


if __name__ == '__main__':
    # 테스트
    from datetime import date
    today = date.today().strftime('%Y-%m-%d')
    print(f"Columns for 1Part: {get_all_columns('1Part')}")
    print(f"Columns for 2Part: {get_all_columns('2Part')}")
