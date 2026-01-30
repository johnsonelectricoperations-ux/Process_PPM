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

    # 2. 불량유형 마스터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS defect_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_type TEXT NOT NULL,
            defect_name TEXT NOT NULL,
            display_order INTEGER,
            is_active INTEGER DEFAULT 1,
            UNIQUE(part_type, defect_name)
        )
    ''')

    # 3. 불량 입력 데이터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS defect_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_date DATE NOT NULL,
            part_type TEXT NOT NULL,
            process_name TEXT NOT NULL,
            tm_no TEXT NOT NULL,
            code INTEGER,
            품명 TEXT,
            defect_name TEXT NOT NULL,
            quantity INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(record_date, part_type, process_name, tm_no, defect_name)
        )
    ''')

    # 인덱스 생성
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tm_master_part ON tm_master(part_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tm_master_규격 ON tm_master(규격)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_defect_records_date ON defect_records(record_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_defect_records_part ON defect_records(part_type, process_name)')

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


def sync_defect_types():
    """Part1, Part2 Excel 파일에서 불량유형 동기화"""
    conn = get_connection()
    cursor = conn.cursor()

    files = {
        '1Part': 'Part1_20260130.xlsx',
        '2Part': 'Part2_20260130.xlsx'
    }

    # 기본 컬럼 (불량유형에서 제외)
    exclude_columns = ['code', 'TM_No', '품명', '합계']

    for part_type, filename in files.items():
        file_path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(file_path):
            print(f"파일 없음: {file_path}")
            continue

        xlsx = pd.ExcelFile(file_path)
        # 첫 번째 시트에서 불량유형 컬럼 추출
        df = pd.read_excel(xlsx, sheet_name=xlsx.sheet_names[0])

        defect_columns = [col for col in df.columns if col not in exclude_columns]

        for order, defect_name in enumerate(defect_columns):
            cursor.execute('''
                INSERT OR IGNORE INTO defect_types (part_type, defect_name, display_order)
                VALUES (?, ?, ?)
            ''', (part_type, defect_name, order))

    conn.commit()
    conn.close()
    print("불량유형 동기화 완료")


def sync_all():
    """모든 Excel 데이터를 DB에 동기화"""
    init_db()
    sync_tm_master()
    sync_defect_types()
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


def get_defect_types(part_type):
    """불량유형 목록 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT defect_name FROM defect_types
        WHERE part_type = ? AND is_active = 1
        ORDER BY display_order
    ''', (part_type,))

    results = cursor.fetchall()
    conn.close()
    return [row['defect_name'] for row in results]


def save_defect_record(record_date, part_type, process_name, tm_no, code, 품명, defects):
    """불량 데이터 저장"""
    conn = get_connection()
    cursor = conn.cursor()

    for defect_name, quantity in defects.items():
        if quantity and int(quantity) > 0:
            cursor.execute('''
                INSERT OR REPLACE INTO defect_records
                (record_date, part_type, process_name, tm_no, code, 품명, defect_name, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (record_date, part_type, process_name, tm_no, code, 품명, defect_name, int(quantity)))

    conn.commit()
    conn.close()


def get_daily_records(record_date, part_type, process_name):
    """일별 불량 기록 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT tm_no, code, 품명, defect_name, quantity
        FROM defect_records
        WHERE record_date = ? AND part_type = ? AND process_name = ?
        ORDER BY tm_no, defect_name
    ''', (record_date, part_type, process_name))

    results = cursor.fetchall()
    conn.close()
    return [dict(row) for row in results]


if __name__ == '__main__':
    sync_all()
