import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_PATH = 'data/ppm.db'
DATA_DIR = 'data'

# Part별 공정 정의
PART_PROCESSES = {
    '1Part': ['성형', '소결', '정형', '압입', '가공', '밴딩', '후처리'],
    '2Part': ['성형', '소결', '정형', '후처리']
}

# Setting이 있는 공정
SETTING_PROCESSES = {
    '1Part': ['성형', '정형', '가공'],
    '2Part': ['성형', '정형', '가공']
}


def get_connection():
    """DB 연결 반환"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """DB 초기화 - 테이블 생성"""
    conn = get_connection()
    cursor = conn.cursor()

    # 1. TM_No 마스터 테이블 (품목 정보)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tm_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_type TEXT NOT NULL,
            규격 TEXT NOT NULL,
            품명 TEXT,
            중량 REAL,
            code_성형 INTEGER,
            code_소결 INTEGER,
            code_정형 INTEGER,
            code_가공 INTEGER,
            code_압입 INTEGER,
            code_밴딩 INTEGER,
            code_후처리 INTEGER,
            UNIQUE(part_type, 규격)
        )
    ''')

    # 2. 불량유형 설정 테이블 (defect_config.xlsx 기반)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS defect_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_type TEXT NOT NULL,
            defect_type TEXT NOT NULL,
            defect_id TEXT NOT NULL,
            defect_name TEXT NOT NULL,
            process_name TEXT NOT NULL,
            display_order INTEGER,
            UNIQUE(part_type, defect_type, defect_id, process_name)
        )
    ''')

    # 3. 불량 입력 데이터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS defect_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_date DATE NOT NULL,
            part_type TEXT NOT NULL,
            process_name TEXT NOT NULL,
            defect_type TEXT NOT NULL,
            tm_no TEXT NOT NULL,
            code INTEGER,
            품명 TEXT,
            defect_id TEXT NOT NULL,
            defect_name TEXT NOT NULL,
            quantity INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(record_date, part_type, process_name, defect_type, tm_no, defect_id)
        )
    ''')

    # 인덱스 생성
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tm_master_part ON tm_master(part_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tm_master_규격 ON tm_master(규격)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_defect_config_lookup ON defect_config(part_type, defect_type, process_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_defect_records_date ON defect_records(record_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_defect_records_lookup ON defect_records(part_type, process_name, defect_type)')

    conn.commit()
    conn.close()
    print("DB 초기화 완료")


def sync_tm_master():
    """TM_No_List.xlsx에서 마스터 데이터 동기화"""
    file_path = os.path.join(DATA_DIR, 'TM_No_List.xlsx')
    if not os.path.exists(file_path):
        print(f"파일 없음: {file_path}")
        return

    conn = get_connection()
    cursor = conn.cursor()

    xlsx = pd.ExcelFile(file_path)

    # 1Part_list → 1Part, 2Part_list → 2Part
    sheet_mapping = {
        '1Part_list': '1Part',
        '2Part_list': '2Part'
    }

    for sheet_name, part_type in sheet_mapping.items():
        if sheet_name not in xlsx.sheet_names:
            continue

        df = pd.read_excel(xlsx, sheet_name=sheet_name)

        for _, row in df.iterrows():
            cursor.execute('''
                INSERT OR REPLACE INTO tm_master
                (part_type, 규격, 품명, 중량, code_성형, code_소결, code_정형, code_가공, code_압입, code_밴딩, code_후처리)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                part_type,
                str(row['규격']),
                row['품명'],
                row['중량'] if pd.notna(row['중량']) else None,
                int(row['code_성형']) if pd.notna(row.get('code_성형')) else None,
                int(row['code_소결']) if pd.notna(row.get('code_소결')) else None,
                int(row['code_정형']) if pd.notna(row.get('code_정형')) else None,
                int(row['code_가공']) if pd.notna(row.get('code_가공')) else None,
                int(row['code_압입']) if pd.notna(row.get('code_압입')) else None,
                int(row['code_밴딩']) if pd.notna(row.get('code_밴딩')) else None,
                int(row['code_후처리']) if pd.notna(row.get('code_후처리')) else None
            ))

    conn.commit()
    conn.close()
    print("TM 마스터 동기화 완료")


def sync_defect_config():
    """defect_config.xlsx에서 공정별 불량유형 설정 동기화"""
    file_path = os.path.join(DATA_DIR, 'defect_config.xlsx')
    if not os.path.exists(file_path):
        print(f"파일 없음: {file_path}")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # 기존 데이터 삭제
    cursor.execute('DELETE FROM defect_config')

    xlsx = pd.ExcelFile(file_path)

    # 시트 매핑: 시트명 → (part_type, defect_type)
    sheet_mapping = {
        '1Part_Setting': ('1Part', 'Setting'),
        '1Part_Process': ('1Part', '공정불량'),
        '2Part_Setting': ('2Part', 'Setting'),
        '2Part_Process': ('2Part', '공정불량')
    }

    for sheet_name, (part_type, defect_type) in sheet_mapping.items():
        if sheet_name not in xlsx.sheet_names:
            continue

        df = pd.read_excel(xlsx, sheet_name=sheet_name)

        # 공정 컬럼 찾기 (ID, 불량명 제외)
        process_columns = [col for col in df.columns if col not in ['ID', '불량명']]

        for order, (_, row) in enumerate(df.iterrows()):
            defect_id = row['ID']
            defect_name = row['불량명']

            for process_name in process_columns:
                # 'Y' 표시된 경우만 추가
                if pd.notna(row.get(process_name)) and str(row.get(process_name)).upper() == 'Y':
                    cursor.execute('''
                        INSERT OR REPLACE INTO defect_config
                        (part_type, defect_type, defect_id, defect_name, process_name, display_order)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (part_type, defect_type, defect_id, defect_name, process_name, order))

    conn.commit()
    conn.close()
    print("불량유형 설정 동기화 완료")


def sync_all():
    """모든 Excel 데이터를 DB에 동기화"""
    init_db()
    sync_tm_master()
    sync_defect_config()
    print("전체 동기화 완료")


def get_tm_list(part_type, search_term=''):
    """TM-No 목록 조회 (자동완성용)"""
    conn = get_connection()
    cursor = conn.cursor()

    if search_term:
        cursor.execute('''
            SELECT 규격, 품명 FROM tm_master
            WHERE part_type = ? AND 규격 LIKE ?
            ORDER BY 규격
            LIMIT 20
        ''', (part_type, f'{search_term}%'))
    else:
        cursor.execute('''
            SELECT 규격, 품명 FROM tm_master
            WHERE part_type = ?
            ORDER BY 규격
            LIMIT 20
        ''', (part_type,))

    results = cursor.fetchall()
    conn.close()
    return [dict(row) for row in results]


def get_tm_info(part_type, tm_no, process_name):
    """특정 TM-No의 정보 및 해당 공정 code 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    code_column = f'code_{process_name}'

    cursor.execute(f'''
        SELECT 규격, 품명, 중량, {code_column} as code
        FROM tm_master
        WHERE part_type = ? AND 규격 = ?
    ''', (part_type, tm_no))

    result = cursor.fetchone()
    conn.close()
    return dict(result) if result else None


def get_defect_types_for_process(part_type, process_name, defect_type):
    """공정별, 불량유형별 불량 목록 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT defect_id, defect_name FROM defect_config
        WHERE part_type = ? AND process_name = ? AND defect_type = ?
        ORDER BY display_order
    ''', (part_type, process_name, defect_type))

    results = cursor.fetchall()
    conn.close()
    return [{'id': row['defect_id'], 'name': row['defect_name']} for row in results]


def get_available_defect_types(part_type, process_name):
    """해당 공정에서 사용 가능한 불량유형 목록 (Setting/공정불량)"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT DISTINCT defect_type FROM defect_config
        WHERE part_type = ? AND process_name = ?
    ''', (part_type, process_name))

    results = cursor.fetchall()
    conn.close()
    return [row['defect_type'] for row in results]


def save_defect_record(record_date, part_type, process_name, defect_type, tm_no, code, 품명, defects):
    """불량 데이터 저장"""
    conn = get_connection()
    cursor = conn.cursor()

    for defect_id, data in defects.items():
        quantity = data.get('quantity', 0) if isinstance(data, dict) else data
        defect_name = data.get('name', '') if isinstance(data, dict) else ''

        if quantity and int(quantity) > 0:
            cursor.execute('''
                INSERT OR REPLACE INTO defect_records
                (record_date, part_type, process_name, defect_type, tm_no, code, 품명, defect_id, defect_name, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (record_date, part_type, process_name, defect_type, tm_no, code, 품명, defect_id, defect_name, int(quantity)))

    conn.commit()
    conn.close()


def get_daily_records(record_date, part_type, process_name, defect_type):
    """일별 불량 기록 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT tm_no, code, 품명, defect_id, defect_name, quantity
        FROM defect_records
        WHERE record_date = ? AND part_type = ? AND process_name = ? AND defect_type = ?
        ORDER BY tm_no, defect_id
    ''', (record_date, part_type, process_name, defect_type))

    results = cursor.fetchall()
    conn.close()
    return [dict(row) for row in results]


def get_daily_records_for_export(record_date, part_type, defect_type):
    """Excel 출력용 일별 기록 조회 (공정별로 그룹핑)"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT process_name, tm_no, code, 품명, defect_id, defect_name, quantity
        FROM defect_records
        WHERE record_date = ? AND part_type = ? AND defect_type = ?
        ORDER BY process_name, tm_no, defect_id
    ''', (record_date, part_type, defect_type))

    results = cursor.fetchall()
    conn.close()
    return [dict(row) for row in results]


if __name__ == '__main__':
    sync_all()
