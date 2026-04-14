from flask import Flask, request, jsonify, render_template
import sqlite3
import os

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'eamcet.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def calculate_probability(user_rank, avg_cutoff):
    # ratio < 1 means user rank is better than cutoff (good)
    # ratio > 1 means user rank is worse than cutoff (bad)
    ratio = user_rank / avg_cutoff

    if ratio <= 0.60:
        return 95
    elif ratio <= 0.70:
        return 90
    elif ratio <= 0.80:
        return 82
    elif ratio <= 0.90:
        return 72
    elif ratio <= 1.00:
        return 60
    elif ratio <= 1.10:
        return 45
    elif ratio <= 1.20:
        return 30
    elif ratio <= 1.35:
        return 18
    else:
        return 10
# ============================================================
# PREDICTION LOGIC
# ============================================================
def predict_colleges(rank, category, gender, branch, limit=50):
    conn = get_db()
    cursor = conn.cursor()

    query = '''
        SELECT
            inst_code,
            college_name,
            place,
            dist_code,
            college_type,
            branch_name,
            year,
            closing_rank
        FROM cutoffs
        WHERE category    = ?
          AND gender      = ?
          AND branch_name LIKE ?
        ORDER BY inst_code, branch_name, year
    '''
    rows = cursor.execute(query, (category, gender, f'%{branch}%')).fetchall()
    conn.close()

    from collections import defaultdict
    groups = defaultdict(list)
    meta   = {}

    for row in rows:
        key = (row['inst_code'], row['branch_name'])
        groups[key].append((row['year'], row['closing_rank']))
        meta[key] = {
            'inst_code'   : row['inst_code'],
            'college_name': row['college_name'],
            'place'       : row['place'],
            'dist_code'   : row['dist_code'],
            'college_type': row['college_type'],
            'branch_name' : row['branch_name'],
        }

    results = []

    for key, year_ranks in groups.items():
        year_ranks.sort(key=lambda x: x[0])
        years_count = len(year_ranks)

        weights = {2020: 1, 2021: 2, 2022: 3, 2023: 4, 2024: 5}
        weighted_sum = sum(rk * weights.get(yr, 1) for yr, rk in year_ranks)
        weight_total = sum(weights.get(yr, 1) for yr, _ in year_ranks)
        avg_cutoff   = weighted_sum / weight_total
        predicted_2025 = int(avg_cutoff)

        if avg_cutoff > rank * 1.8 or avg_cutoff < rank * 0.5:
            continue

        ratio = rank / avg_cutoff

        if ratio <= 0.80:
            chance = 'SAFE'
            label  = 'Safe'
        elif ratio <= 1.05:
            chance = 'TARGET'
            label  = 'Target'
        elif ratio <= 1.50:
            chance = 'DREAM'
            label  = 'Dream'
        else:
            continue

        score = min(100, int((avg_cutoff / rank) * 100))

        trend = 'Stable'
        if len(year_ranks) >= 2:
            recent   = year_ranks[-1][1]
            previous = year_ranks[-2][1]
            diff = recent - previous
            if diff < -1000:
                trend = 'Getting Harder'
            elif diff > 1000:
                trend = 'Getting Easier'

        explanation = (
            f"Your rank {rank:,} vs avg cutoff {int(avg_cutoff):,} "
            f"over {years_count} year(s) → ratio {ratio:.2f}"
        )

        probability = calculate_probability(rank, int(avg_cutoff))

        results.append({
            'inst_code'     : meta[key]['inst_code'],
            'college_name'  : meta[key]['college_name'],
            'place'         : meta[key]['place'],
            'dist_code'     : meta[key]['dist_code'],
            'college_type'  : meta[key]['college_type'],
            'branch_name'   : meta[key]['branch_name'],
            'avg_cutoff'    : int(avg_cutoff),
            'years_count'   : years_count,
            'chance'        : chance,
            'label'         : label,
            'score'         : score,
            'ratio'         : round(ratio, 2),
            'trend'         : trend,
            'explanation'   : explanation,
            'predicted_2025': predicted_2025,
            'probability'   : probability,
            'yearly_data'   : [
                {'year': yr, 'rank': rk}
                for yr, rk in sorted(year_ranks)
            ],
        })

    results.sort(key=lambda x: x['avg_cutoff'])
    return results[:limit]

# ============================================================
# ROUTES
# ============================================================

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/predict', methods=['POST'])
def predict():
    data = request.get_json()

    # Validate inputs
    rank     = data.get('rank')
    category = data.get('category', '').upper().strip()
    gender   = data.get('gender', '').upper().strip()
    branch   = data.get('branch', '').strip()

    if not rank or not category or not gender or not branch:
        return jsonify({'error': 'Please fill all fields'}), 400

    try:
        rank = int(rank)
    except:
        return jsonify({'error': 'Rank must be a number'}), 400

    if rank <= 0 or rank > 200000:
        return jsonify({'error': 'Enter a valid rank between 1 and 200000'}), 400

    results = predict_colleges(rank, category, gender, branch)

    return jsonify({
        'rank'     : rank,
        'category' : category,
        'gender'   : gender,
        'branch'   : branch,
        'count'    : len(results),
        'results'  : results
    })


@app.route('/api/branches', methods=['GET'])
def get_branches():
    conn = get_db()
    cursor = conn.cursor()
    rows = cursor.execute(
        'SELECT DISTINCT branch_name FROM cutoffs ORDER BY branch_name'
    ).fetchall()
    conn.close()
    branches = [r['branch_name'] for r in rows]
    return jsonify(branches)


@app.route('/api/categories', methods=['GET'])
def get_categories():
    conn = get_db()
    cursor = conn.cursor()
    rows = cursor.execute(
        'SELECT DISTINCT category FROM cutoffs ORDER BY category'
    ).fetchall()
    conn.close()
    categories = [r['category'] for r in rows]
    return jsonify(categories)
@app.route('/api/trends', methods=['GET'])
def get_trends():
    inst_code = request.args.get('inst_code', '')
    branch    = request.args.get('branch', '')
    category  = request.args.get('category', 'OC')
    gender    = request.args.get('gender', 'BOYS')

    conn = get_db()
    cursor = conn.cursor()
    rows = cursor.execute('''
        SELECT year, closing_rank
        FROM cutoffs
        WHERE inst_code = ?
          AND branch_name LIKE ?
          AND category = ?
          AND gender = ?
        ORDER BY year ASC
    ''', (inst_code, f'%{branch}%', category, gender)).fetchall()
    conn.close()

    return jsonify([{'year': r['year'], 'rank': r['closing_rank']} for r in rows])


@app.route('/api/strategy', methods=['POST'])
def generate_strategy():
    data     = request.get_json()
    rank     = int(data.get('rank', 0))
    category = data.get('category', 'OC')
    gender   = data.get('gender', 'BOYS')
    branch   = data.get('branch', '')

    results = predict_colleges(rank, category, gender, branch, limit=50)

    if not results:
        return jsonify({'error': 'No colleges found to generate strategy'}), 404

    # Separate into buckets
    safe   = [r for r in results if r['label'] == 'Safe']
    target = [r for r in results if r['label'] == 'Target']
    dream  = [r for r in results if r['label'] == 'Dream']

    # Reliable = 2+ years of data
    reliable_safe   = [r for r in safe   if r['years_count'] >= 2]
    reliable_target = [r for r in target if r['years_count'] >= 2]
    unreliable      = [r for r in results if r['years_count'] < 2]

    # Risk assessment
    total = len(results)
    if len(safe) >= 5:
        risk_level = 'LOW'
        risk_msg   = 'You have strong options. Good position for counselling.'
    elif len(safe) >= 2:
        risk_level = 'MEDIUM'
        risk_msg   = 'Limited safe options. Apply carefully in Round 1.'
    else:
        risk_level = 'HIGH'
        risk_msg   = 'Very few safe options. Consider backup branches.'

    # Build strategy
    strategy = {
        'rank'         : rank,
        'category'     : category,
        'branch'       : branch,
        'risk_level'   : risk_level,
        'risk_msg'     : risk_msg,
        'total_options': total,
        'round1'       : reliable_safe[:3],    # top 3 safe reliable colleges
        'round2'       : reliable_target[:3],  # top 3 target colleges
        'spot'         : dream[:2],            # top 2 dream shots
        'skip'         : unreliable[:5],       # colleges to skip (bad data)
        'summary'      : f"Based on your rank {rank:,} ({category}), you have "
                         f"{len(safe)} safe, {len(target)} target, and {len(dream)} dream options. "
                         f"Overall admission risk: {risk_level}."
    }

    return jsonify(strategy)

@app.route('/api/districts', methods=['GET'])
def get_districts():
    conn = get_db()
    cursor = conn.cursor()
    rows = cursor.execute(
        'SELECT DISTINCT dist_code FROM cutoffs WHERE dist_code IS NOT NULL ORDER BY dist_code'
    ).fetchall()
    conn.close()
    return jsonify([r['dist_code'] for r in rows])


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    print("="*50)
    print("EAMCET College Predictor - Starting...")
    print("Open your browser and go to: http://localhost:5000")
    print("="*50)

    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))