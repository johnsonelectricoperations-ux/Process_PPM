import pandas as pd
import sqlite3
import os
from datetime import datetime
from collections import defaultdict

DB_PATH = 'data/ppm.db'
OUTPUT_DIR = 'output'
DATA_DIR = 'data'

# Part별 공정 정의
PART_PROCESSES = {
    '1Part': ['성형', '소결', '정형', '압입', '가공', '밴딩', '후처리'],
    '2Part': ['성형', '소결', '정형', '후처리']
}

# Output_Master 시트 매핑
OUTPUT_MASTER_SHEETS = {
    ('1Part', 'Setting'): '1Part_Setting',
    ('1Part', '공정불량'): '1Part_Process',
    ('2Part', 'Setting'): '2Part_Setting',
    ('2Part', '공정불량'): '2Part_Process'
}


def get_output_template(part_type, defect_type):
    """Output_Master.xlsx에서 컬럼 구조 가져오기"""
    template_file = os.path.join(DATA_DIR, 'Output_Master.xlsx')
    if not os.path.exists(template_file):
        raise FileNotFoundError(f"템플릿 파일 없음: {template_file}")

    sheet_name = OUTPUT_MASTER_SHEETS.get((part_type, defect_type))
    if not sheet_name:
        raise ValueError(f"Unknown part_type/defect_type: {part_type}/{defect_type}")

    # 헤더는 1행, ID는 2행
    df = pd.read_excel(template_file, sheet_name=sheet_name, header=None)

    # 1행: 컬럼명 (code, TM-No, 품명, 합계, 불량명들...)
    columns = df.iloc[0].tolist()

    # 2행: ID 코드 (참조용, 불량 컬럼만 해당)
    id_row = df.iloc[1].tolist()

    # 불량 컬럼 매핑 (ID → 컬럼명)
    defect_columns = {}
    for i, (col, defect_id) in enumerate(zip(columns[4:], id_row[4:]), start=4):
        if pd.notna(defect_id):
            defect_columns[str(defect_id)] = col

    return columns, defect_columns


def get_processes_for_defect_type(part_type, defect_type):
    """해당 defect_type에 대해 데이터가 있는 공정 목록"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT DISTINCT process_name FROM defect_config
        WHERE part_type = ? AND defect_type = ?
    ''', (part_type, defect_type))

    results = cursor.fetchall()
    conn.close()

    # 정의된 순서대로 정렬
    all_processes = PART_PROCESSES.get(part_type, [])
    config_processes = [r[0] for r in results]
    return [p for p in all_processes if p in config_processes]


def export_daily_excel(record_date, part_type):
    """일별 Excel 파일 생성 (Setting, 공정불량 각각)"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    date_str = record_date.replace('-', '')
    part_num = '1' if part_type == '1Part' else '2'

    filenames = []

    # Setting과 공정불량 각각 파일 생성
    for defect_type in ['Setting', '공정불량']:
        try:
            columns, defect_columns = get_output_template(part_type, defect_type)
        except (FileNotFoundError, ValueError) as e:
            print(f"템플릿 오류: {e}")
            continue

        processes = get_processes_for_defect_type(part_type, defect_type)
        if not processes:
            continue

        # 파일명 생성
        type_suffix = 'Setting' if defect_type == 'Setting' else '공정불량'
        filename = f'Part{part_num}_{date_str}_{type_suffix}.xlsx'
        filepath = os.path.join(OUTPUT_DIR, filename)

        # Excel Writer 생성
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            for process_name in processes:
                # 해당 공정의 데이터 조회
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT tm_no, code, 품명, defect_id, quantity
                    FROM defect_records
                    WHERE record_date = ? AND part_type = ? AND process_name = ? AND defect_type = ?
                    ORDER BY tm_no
                ''', (record_date, part_type, process_name, defect_type))

                records = cursor.fetchall()

                # 데이터를 피벗 형태로 변환
                data_dict = {}
                for row in records:
                    tm_no = row['tm_no']
                    if tm_no not in data_dict:
                        data_dict[tm_no] = {
                            'code': row['code'],
                            'TM-No': tm_no,
                            '품명': row['품명'],
                            '합계': 0
                        }
                        # 불량 컬럼 초기화
                        for col_name in defect_columns.values():
                            data_dict[tm_no][col_name] = 0

                    defect_id = str(row['defect_id'])
                    quantity = row['quantity'] or 0

                    # defect_id를 컬럼명으로 변환
                    if defect_id in defect_columns:
                        col_name = defect_columns[defect_id]
                        data_dict[tm_no][col_name] = quantity
                        data_dict[tm_no]['합계'] += quantity

                # DataFrame 생성
                if data_dict:
                    df = pd.DataFrame(list(data_dict.values()))
                    # 컬럼 순서 맞추기
                    df = df.reindex(columns=columns, fill_value=0)
                else:
                    # 빈 DataFrame (컬럼만 있는)
                    df = pd.DataFrame(columns=columns)

                # 시트에 저장
                df.to_excel(writer, sheet_name=process_name, index=False)

        filenames.append(filename)

    conn.close()
    return filenames


def get_daily_summary(record_date, part_type):
    """일별 요약 데이터 조회"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT process_name, defect_type, COUNT(DISTINCT tm_no) as item_count, SUM(quantity) as total_defects
        FROM defect_records
        WHERE record_date = ? AND part_type = ?
        GROUP BY process_name, defect_type
    ''', (record_date, part_type))

    results = cursor.fetchall()
    conn.close()

    return [{'process': row[0], 'defect_type': row[1], 'items': row[2], 'defects': row[3]} for row in results]


if __name__ == '__main__':
    # 테스트
    from datetime import date
    today = date.today().strftime('%Y-%m-%d')

    try:
        columns, defect_cols = get_output_template('1Part', 'Setting')
        print(f"1Part_Setting 컬럼: {columns}")
        print(f"불량ID 매핑: {defect_cols}")
    except Exception as e:
        print(f"오류: {e}")
