from flask import Flask, request, jsonify, render_template
import sqlite3
import os
import math

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), 'eamcet.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def calculate_probability(rank, avg_cutoff):
    ratio = rank / avg_cutoff
    prob = 1 / (1 + math.exp(10 * (ratio - 1)))
    return round(prob * 100)

def predict_colleges(rank, category, gender, branch, limit=50, district='', mode='branch'):
    conn = get_db()
    cursor = conn.cursor()

    if mode == 'college':
        search = f'%{branch}%'
        if district:
            rows = cursor.execute('''SELECT inst_code, college_name, place, dist_code, college_type, branch_name, year, closing_rank FROM cutoffs WHERE category=? AND gender=? AND (college_name LIKE ? OR inst_code LIKE ?) AND dist_code=? ORDER BY inst_code, branch_name, year''', (category, gender, search, search, district)).fetchall()
        else:
            rows = cursor.execute('''SELECT inst_code, college_name, place, dist_code, college_type, branch_name, year, closing_rank FROM cutoffs WHERE category=? AND gender=? AND (college_name LIKE ? OR inst_code LIKE ?) ORDER BY inst_code, branch_name, year''', (category, gender, search, search)).fetchall()
    elif mode == 'any':
        if district:
            rows = cursor.execute('''SELECT inst_code, college_name, place, dist_code, college_type, branch_name, year, closing_rank FROM cutoffs WHERE category=? AND gender=? AND dist_code=? ORDER BY inst_code, branch_name, year''', (category, gender, district)).fetchall()
        else:
            rows = cursor.execute('''SELECT inst_code, college_name, place, dist_code, college_type, branch_name, year, closing_rank FROM cutoffs WHERE category=? AND gender=? ORDER BY inst_code, branch_name, year''', (category, gender)).fetchall()
    else:
        if district:
            rows = cursor.execute('''SELECT inst_code, college_name, place, dist_code, college_type, branch_name, year, closing_rank FROM cutoffs WHERE category=? AND gender=? AND branch_name=? AND dist_code=? ORDER BY inst_code, branch_name, year''', (category, gender, branch, district)).fetchall()
        else:
            rows = cursor.execute('''SELECT inst_code, college_name, place, dist_code, college_type, branch_name, year, closing_rank FROM cutoffs WHERE category=? AND gender=? AND branch_name=? ORDER BY inst_code, branch_name, year''', (category, gender, branch)).fetchall()
    conn.close()

    from collections import defaultdict
    groups = defaultdict(list)
    meta = {}
    for row in rows:
        key = (row['inst_code'], row['branch_name'])
        groups[key].append((row['year'], row['closing_rank']))
        meta[key] = {'inst_code': row['inst_code'], 'college_name': row['college_name'], 'place': row['place'], 'dist_code': row['dist_code'], 'college_type': row['college_type'], 'branch_name': row['branch_name']}

    results = []
    for key, year_ranks in groups.items():
        year_ranks.sort(key=lambda x: x[0])
        years_count = len(year_ranks)
        weights = {2020:1, 2021:2, 2022:3, 2023:4, 2024:5}
        weighted_sum = sum(rk * weights.get(yr, 1) for yr, rk in year_ranks)
        weight_total = sum(weights.get(yr, 1) for yr, _ in year_ranks)
        avg_cutoff = weighted_sum / weight_total
        
        # More lenient filtering - allow wider range for low ranks
        if rank < 2000:
            # For very low ranks, be more generous
            if avg_cutoff > rank * 3.0 or avg_cutoff < rank * 0.3:
                continue
        else:
            # For higher ranks, use standard range
            if avg_cutoff > rank * 1.8 or avg_cutoff < rank * 0.5:
                continue
        
        ratio = rank / avg_cutoff
        if ratio <= 0.80:      label = 'Safe'
        elif ratio <= 1.05:    label = 'Target'
        elif ratio <= 2.0:     label = 'Dream'
        else:                  continue
        trend = 'Stable'
        if len(year_ranks) >= 2:
            diff = year_ranks[-1][1] - year_ranks[-2][1]
            if diff < -1000:   trend = 'Getting Harder'
            elif diff > 1000:  trend = 'Getting Easier'
        results.append({
            'inst_code': meta[key]['inst_code'], 'college_name': meta[key]['college_name'],
            'place': meta[key]['place'], 'dist_code': meta[key]['dist_code'],
            'college_type': meta[key]['college_type'], 'branch_name': meta[key]['branch_name'],
            'avg_cutoff': int(avg_cutoff), 'years_count': years_count,
            'chance': label.upper(), 'label': label,
            'score': min(100, int((avg_cutoff / rank) * 100)),
            'ratio': round(ratio, 2), 'trend': trend,
            'explanation': f"Your rank {rank:,} vs avg cutoff {int(avg_cutoff):,} over {years_count} year(s) → ratio {ratio:.2f}",
            'predicted_2025': int(avg_cutoff),
            'probability': calculate_probability(rank, int(avg_cutoff)),
            'yearly_data': [{'year': yr, 'rank': rk} for yr, rk in sorted(year_ranks)],
        })
    results.sort(key=lambda x: x['avg_cutoff'])
    return results[:limit]

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/predict', methods=['POST'])
def predict():
    data = request.get_json()
    rank     = data.get('rank')
    category = data.get('category', '').upper().strip()
    gender   = data.get('gender', '').upper().strip()
    branch   = data.get('branch', '').strip()
    district = data.get('district', '').strip().upper()
    mode     = data.get('mode', 'branch')
    if mode == 'branch' and not branch:
        return jsonify({'error': 'Please select a branch'}), 400
    if mode == 'college' and not branch:
        return jsonify({'error': 'Please enter a college name'}), 400
    if not rank or not category or not gender:
        return jsonify({'error': 'Please fill all fields'}), 400
    try:
        rank = int(rank)
    except:
        return jsonify({'error': 'Rank must be a number'}), 400
    if rank <= 0 or rank > 200000:
        return jsonify({'error': 'Enter a valid rank between 1 and 200000'}), 400
    results = predict_colleges(rank, category, gender, branch, district=district, mode=mode)
    return jsonify({'rank': rank, 'category': category, 'gender': gender, 'branch': branch, 'count': len(results), 'results': results})

@app.route('/api/branches', methods=['GET'])
def get_branches():
    conn = get_db()
    rows = conn.execute('SELECT DISTINCT branch_name FROM cutoffs ORDER BY branch_name').fetchall()
    conn.close()
    return jsonify([r['branch_name'] for r in rows])

@app.route('/api/categories', methods=['GET'])
def get_categories():
    conn = get_db()
    rows = conn.execute('SELECT DISTINCT category FROM cutoffs ORDER BY category').fetchall()
    conn.close()
    return jsonify([r['category'] for r in rows])

@app.route('/api/trends', methods=['GET'])
def get_trends():
    inst_code = request.args.get('inst_code', '')
    branch    = request.args.get('branch', '')
    category  = request.args.get('category', 'OC')
    gender    = request.args.get('gender', 'BOYS')
    conn = get_db()
    rows = conn.execute('''SELECT year, closing_rank FROM cutoffs WHERE inst_code=? AND branch_name=? AND category=? AND gender=? ORDER BY year ASC''', (inst_code, branch, category, gender)).fetchall()
    conn.close()
    return jsonify([{'year': r['year'], 'rank': r['closing_rank']} for r in rows])

@app.route('/api/strategy', methods=['POST'])
def generate_strategy():
    try:
        data     = request.get_json()
        rank     = int(data.get('rank', 0))
        category = data.get('category', 'OC')
        gender   = data.get('gender', 'BOYS')
        branch   = data.get('branch', '')
        mode     = data.get('mode', 'branch')
        results  = predict_colleges(rank, category, gender, branch, limit=50, mode=mode)
        if not results:
            return jsonify({'error': 'No colleges found to generate strategy'}), 404
        safe   = [r for r in results if r['label'] == 'Safe']
        target = [r for r in results if r['label'] == 'Target']
        dream  = [r for r in results if r['label'] == 'Dream']
        reliable_safe   = [r for r in safe   if r['years_count'] >= 2]
        reliable_target = [r for r in target if r['years_count'] >= 2]
        unreliable      = [r for r in results if r['years_count'] < 2]
        risk_level = 'LOW' if len(safe) >= 5 else 'MEDIUM' if len(safe) >= 2 else 'HIGH'
        risk_msg   = {'LOW': 'You have strong options. Good position for counselling.', 'MEDIUM': 'Limited safe options. Apply carefully in Round 1.', 'HIGH': 'Very few safe options. Consider backup branches.'}[risk_level]
        return jsonify({'rank': rank, 'category': category, 'branch': branch, 'risk_level': risk_level, 'risk_msg': risk_msg, 'total_options': len(results), 'round1': reliable_safe[:3], 'round2': reliable_target[:3], 'spot': dream[:2], 'skip': unreliable[:5], 'summary': f"Based on your rank {rank:,} ({category}), you have {len(safe)} safe, {len(target)} target, and {len(dream)} dream options. Overall admission risk: {risk_level}."})
    except Exception as e:
        return jsonify({'error': f'Strategy generation failed: {str(e)}'}), 500

@app.route('/api/districts', methods=['GET'])
def get_districts():
    conn = get_db()
    rows = conn.execute('SELECT DISTINCT dist_code FROM cutoffs WHERE dist_code IS NOT NULL ORDER BY dist_code').fetchall()
    conn.close()
    return jsonify([r['dist_code'] for r in rows])

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
