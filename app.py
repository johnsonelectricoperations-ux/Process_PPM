from flask import Flask, render_template, request, jsonify
from datetime import datetime, date
import database as db
from excel_export import export_daily_excel

app = Flask(__name__)

# Part별 공정 정의
PART_PROCESSES = {
    '1Part': ['성형', '소결', '정형', '압입', '가공', '밴딩', '후처리'],
    '2Part': ['성형', '소결', '정형', '후처리']
}


@app.route('/')
def index():
    """메인 페이지"""
    today = date.today().strftime('%Y-%m-%d')
    return render_template('index.html', today=today, part_processes=PART_PROCESSES)


@app.route('/api/processes/<part_type>')
def get_processes(part_type):
    """Part별 공정 목록 반환"""
    processes = PART_PROCESSES.get(part_type, [])
    return jsonify(processes)


@app.route('/api/tm_search')
def tm_search():
    """TM-No 자동완성 검색"""
    part_type = request.args.get('part_type', '1Part')
    search_term = request.args.get('term', '')

    results = db.get_tm_list(part_type, search_term)
    return jsonify(results)


@app.route('/api/tm_info')
def tm_info():
    """TM-No 상세 정보 조회"""
    part_type = request.args.get('part_type', '1Part')
    tm_no = request.args.get('tm_no', '')
    process_name = request.args.get('process', '')

    if not tm_no or not process_name:
        return jsonify({'error': 'Missing parameters'}), 400

    info = db.get_tm_info(part_type, tm_no, process_name)
    return jsonify(info) if info else jsonify({'error': 'Not found'}), 404


@app.route('/api/defect_types/<part_type>')
def defect_types(part_type):
    """불량유형 목록 반환"""
    types = db.get_defect_types(part_type)
    return jsonify(types)


@app.route('/api/save', methods=['POST'])
def save_record():
    """불량 데이터 저장"""
    data = request.json

    record_date = data.get('date')
    part_type = data.get('part_type')
    process_name = data.get('process')
    tm_no = data.get('tm_no')
    code = data.get('code')
    품명 = data.get('품명')
    defects = data.get('defects', {})

    if not all([record_date, part_type, process_name, tm_no]):
        return jsonify({'error': 'Missing required fields'}), 400

    try:
        db.save_defect_record(record_date, part_type, process_name, tm_no, code, 품명, defects)
        return jsonify({'success': True, 'message': '저장 완료'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/export', methods=['POST'])
def export_excel():
    """일별 Excel 파일 생성"""
    data = request.json
    record_date = data.get('date')
    part_type = data.get('part_type')

    if not record_date or not part_type:
        return jsonify({'error': 'Missing parameters'}), 400

    try:
        filename = export_daily_excel(record_date, part_type)
        return jsonify({'success': True, 'filename': filename})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sync', methods=['POST'])
def sync_db():
    """Excel에서 DB 동기화 (새로고침)"""
    try:
        db.sync_all()
        return jsonify({'success': True, 'message': 'DB 동기화 완료'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/daily_records')
def daily_records():
    """일별 기록 조회"""
    record_date = request.args.get('date')
    part_type = request.args.get('part_type')
    process_name = request.args.get('process')

    if not all([record_date, part_type, process_name]):
        return jsonify({'error': 'Missing parameters'}), 400

    records = db.get_daily_records(record_date, part_type, process_name)
    return jsonify(records)


if __name__ == '__main__':
    # 시작 시 DB 동기화
    db.sync_all()
    app.run(debug=True, host='0.0.0.0', port=5000)
