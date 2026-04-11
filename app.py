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
        ranks_only  = [r for _, r in year_ranks]
        avg_cutoff  = sum(ranks_only) / len(ranks_only)

        # Skip colleges where cutoff is more than 5x the student rank
        # These are irrelevant results
        if avg_cutoff > rank * 5 or avg_cutoff < rank * 0.2:
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
            'inst_code'   : meta[key]['inst_code'],
            'college_name': meta[key]['college_name'],
            'place'       : meta[key]['place'],
            'dist_code'   : meta[key]['dist_code'],
            'college_type': meta[key]['college_type'],
            'branch_name' : meta[key]['branch_name'],
            'avg_cutoff'  : int(avg_cutoff),
            'years_count' : years_count,
            'chance'      : chance,
            'label'       : label,
            'score'       : score,
            'ratio'       : round(ratio, 2),
            'trend'       : trend,
            'explanation' : explanation,


            'probability' : probability,
            'yearly_data': [
    {'year': yr, 'rank': rk} 
    for yr, rk in sorted(year_ranks)
],


        })

    results.sort(key=lambda x: x['probability'], reverse=True)
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


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    print("="*50)
    print("EAMCET College Predictor - Starting...")
    print("Open your browser and go to: http://localhost:5000")
    print("="*50)

    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))